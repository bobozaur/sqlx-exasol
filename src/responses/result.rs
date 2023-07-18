use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use either::Either;
use futures_util::{
    future::{self, BoxFuture, Ready},
    stream::{self, Once},
    Future, FutureExt, Stream,
};
use pin_project::pin_project;
use serde::Deserialize;
use serde_json::Value;

use crate::{
    column::ExaColumn, command::Fetch, connection::ExaConnection, query_result::ExaQueryResult,
    row::ExaRow,
};

use super::fetched::DataChunk;

type NextChunkFuture<'a> = BoxFuture<'a, Result<(DataChunk, &'a mut ExaConnection), String>>;

const NUM_BYTES: usize = 5 * 1024 * 1024;

#[pin_project]
pub struct ExaResultStream<'a, F>
where
    F: Future<Output = Result<ExecutionResultsStream<'a>, String>>,
{
    #[pin]
    state: ExaResultStreamState<'a, F>,
    had_err: bool,
}

#[pin_project(project = ExaResultStreamStateProj, project_replace = ExaResultStreamStateProjReplace)]
enum ExaResultStreamState<'a, F>
where
    F: Future<Output = Result<ExecutionResultsStream<'a>, String>>,
{
    Initial(#[pin] F),
    Stream(ExecutionResultsStream<'a>),
}

impl<'a, F> ExaResultStream<'a, F>
where
    F: Future<Output = Result<ExecutionResultsStream<'a>, String>>,
{
    pub fn new(future: F) -> Self {
        Self {
            state: ExaResultStreamState::Initial(future),
            had_err: false,
        }
    }

    fn from_parts(state: ExaResultStreamState<'a, F>, had_err: bool) -> Self {
        Self { state, had_err }
    }

    fn poll_next_impl(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<<Self as Stream>::Item>> {
        let mut this = self.as_mut().project();
        let state = this.state.as_mut().project();

        match state {
            ExaResultStreamStateProj::Stream(stream) => Pin::new(stream).poll_next(cx),
            ExaResultStreamStateProj::Initial(fut) => {
                let Poll::Ready(stream) = fut.poll(cx)? else {return Poll::Pending};
                let state = ExaResultStreamState::Stream(stream);
                self.set(Self::from_parts(state, self.had_err));
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}

impl<'a, F> Stream for ExaResultStream<'a, F>
where
    F: Future<Output = Result<ExecutionResultsStream<'a>, String>>,
{
    type Item = Result<Either<ExaQueryResult, ExaRow>, String>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.had_err {
            return Poll::Ready(None);
        }

        let Poll::Ready(opt_res) = self.as_mut().poll_next_impl(cx) else {return Poll::Pending};
        let Some(res) = opt_res else { return Poll::Ready(None)};
        *self.project().had_err = res.is_err();
        Poll::Ready(Some(res))
    }
}

/// Struct used for deserialization of the JSON
/// returned after executing one or more queries
/// Represents the collection of results from all queries.
#[allow(unused)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionResults {
    num_results: u8,
    results: Vec<QueryResult>,
}

impl ExecutionResults {
    fn handles(&self) -> Option<Vec<u16>> {
        self.results.iter().map(|qr| qr.handle()).collect()
    }
}

pub struct ExecutionResultsStream<'a> {
    results: std::vec::IntoIter<QueryResult>,
    current_stream: QueryResultStream<'a>,
}

impl<'a> ExecutionResultsStream<'a> {
    pub fn new(con: &'a mut ExaConnection, results: ExecutionResults) -> Result<Self, String> {
        con.last_result_set_handles = results.handles();
        let mut results = results.results.into_iter();

        let query_result = results
            .next()
            .ok_or_else(|| "Query returned no results returned".to_owned())?;

        let stream = QueryResultStream::new(con, query_result)?;

        Ok(Self {
            results,
            current_stream: stream,
        })
    }

    fn renew_stream(&mut self, con: &'a mut ExaConnection) -> Result<(), String> {
        if let Some(query_result) = self.results.next() {
            self.current_stream = QueryResultStream::new(con, query_result)?;
        }

        Ok(())
    }

    fn renew_stream_and_pend(
        &mut self,
        cx: &mut Context<'_>,
        con: &'a mut ExaConnection,
    ) -> Poll<Option<<Self as Stream>::Item>> {
        match self.renew_stream(con) {
            Ok(_) => wake_and_pend(cx),
            Err(e) => Poll::Ready(Some(Err(e))),
        }
    }

    fn renew_stream_with_result(
        &mut self,
        con: &'a mut ExaConnection,
        qr: ExaQueryResult,
    ) -> Poll<Option<<Self as Stream>::Item>> {
        Poll::Ready(Some(self.renew_stream(con).map(|_| Either::Left(qr))))
    }
}

impl<'a> Stream for ExecutionResultsStream<'a> {
    type Item = Result<Either<ExaQueryResult, ExaRow>, String>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let Poll::Ready(opt) = Pin::new(&mut self.current_stream).poll_next(cx)? else {return Poll::Pending};
        let Some(either) = opt else {return Poll::Ready(None)};

        match either {
            Either::Left((qr, con)) => self.renew_stream_with_result(con, qr),
            Either::Right(Either::Left(row)) => Poll::Ready(Some(Ok(Either::Right(row)))),
            Either::Right(Either::Right(con)) => self.renew_stream_and_pend(cx, con),
        }
    }
}

/// Struct used for deserialization of the JSON
/// returned sending queries to the database.
/// Represents the result of one query.
#[allow(non_snake_case)]
#[derive(Debug, Deserialize)]
#[serde(tag = "resultType", rename_all = "camelCase")]
enum QueryResult {
    #[serde(rename_all = "camelCase")]
    ResultSet { result_set: ResultSet },
    #[serde(rename_all = "camelCase")]
    RowCount { row_count: u64 },
}

impl QueryResult {
    fn handle(&self) -> Option<u16> {
        match self {
            QueryResult::ResultSet { result_set } => result_set.result_set_handle,
            QueryResult::RowCount { .. } => None,
        }
    }
}

enum QueryResultStream<'a> {
    ResultSet(ResultSetStream<'a>),
    RowCount(Once<Ready<Result<(ExaQueryResult, &'a mut ExaConnection), String>>>),
}

impl<'a> QueryResultStream<'a> {
    fn new(con: &'a mut ExaConnection, query_result: QueryResult) -> Result<Self, String> {
        let stream = match query_result {
            QueryResult::ResultSet { result_set } => Self::make_result_set_stream(result_set, con)?,
            QueryResult::RowCount { row_count } => Self::make_row_count_stream(row_count, con),
        };

        Ok(stream)
    }

    fn make_row_count_stream(row_count: u64, con: &'a mut ExaConnection) -> Self {
        let output = (ExaQueryResult::new(row_count), con);
        let stream = stream::once(future::ready(Ok(output)));
        Self::RowCount(stream)
    }

    fn make_result_set_stream(rs: ResultSet, con: &'a mut ExaConnection) -> Result<Self, String> {
        let stream = ResultSetStream::new(rs, con)?;
        Ok(Self::ResultSet(stream))
    }
}

impl<'a> Stream for QueryResultStream<'a> {
    type Item = Result<
        Either<(ExaQueryResult, &'a mut ExaConnection), Either<ExaRow, &'a mut ExaConnection>>,
        String,
    >;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        macro_rules! poll_map {
            ($val:expr, $var:ident) => {
                Pin::new($val)
                    .poll_next(cx)
                    .map(|o| o.map(|r| r.map(Either::$var)))
            };
        }

        match self.get_mut() {
            QueryResultStream::RowCount(rc) => poll_map!(rc, Left),
            QueryResultStream::ResultSet(rs) => poll_map!(rs, Right),
        }
    }
}

/// Struct representing a database result set.
/// You'll generally only interact with this if you need information about result set columns.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResultSet {
    #[serde(rename = "numRows")]
    total_rows_num: usize,
    result_set_handle: Option<u16>,
    columns: Vec<ExaColumn>,
    #[serde(flatten)]
    data_chunk: DataChunk,
}

// #[derive(Debug)]
struct ResultSetStream<'a> {
    chunk_iter: ChunkIter,
    chunk_stream: ChunkStream<'a>,
}

impl<'a> ResultSetStream<'a> {
    fn new(rs: ResultSet, con: &'a mut ExaConnection) -> Result<Self, String> {
        let pos = rs.data_chunk.len();

        let fetch_future = if pos < rs.total_rows_num {
            let handle = rs
                .result_set_handle
                .ok_or_else(|| "Missing result set handle".to_owned())?;

            Some(ChunkFuture::new_next_chunk(con, handle, pos, NUM_BYTES))
        } else {
            Some(ChunkFuture::End(con))
        };

        let chunk_stream = ChunkStream {
            total_rows_num: rs.total_rows_num,
            total_rows_pos: pos,
            fetch_future,
        };

        let chunk_iter = ChunkIter {
            columns: rs.columns.into(),
            chunk_rows_total: rs.data_chunk.chunk_rows_num,
            chunk_rows_pos: 0,
            data: rs.data_chunk.data.into(),
        };

        let stream = Self {
            chunk_iter,
            chunk_stream,
        };

        Ok(stream)
    }

    fn renew_chunk_and_pend<T>(&mut self, cx: &mut Context<'_>, chunk: DataChunk) -> Poll<T> {
        self.chunk_iter.renew(chunk);
        wake_and_pend(cx)
    }
}

impl<'a> Stream for ResultSetStream<'a> {
    type Item = Result<Either<ExaRow, &'a mut ExaConnection>, String>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(row) = self.chunk_iter.next() {
            return Poll::Ready(Some(Ok(Either::Left(row))));
        }

        let Poll::Ready(opt) = Pin::new(&mut self.chunk_stream).poll_next(cx)? else {return Poll::Pending};
        let Some(either) = opt else {return Poll::Ready(None)};

        match either {
            Either::Left(chunk) => self.renew_chunk_and_pend(cx, chunk),
            Either::Right(con) => Poll::Ready(Some(Ok(Either::Right(con)))),
        }
    }
}

struct ChunkStream<'a> {
    fetch_future: Option<ChunkFuture<'a>>,
    total_rows_num: usize,
    total_rows_pos: usize,
}

impl<'a> ChunkStream<'a> {
    fn make_future(&self, con: &'a mut ExaConnection, handle: u16) -> ChunkFuture<'a> {
        if self.total_rows_pos < self.total_rows_num {
            ChunkFuture::new_next_chunk(con, handle, self.total_rows_pos, NUM_BYTES)
        } else {
            ChunkFuture::End(con)
        }
    }

    fn poll_and_process_future<F>(
        &mut self,
        mut future: F,
        handle: u16,
        cx: &mut Context<'_>,
    ) -> Poll<Option<<Self as Stream>::Item>>
    where
        F: Future<Output = Result<(DataChunk, &'a mut ExaConnection), String>> + Unpin,
    {
        let Poll::Ready((chunk, con)) = future.poll_unpin(cx)? else {return Poll::Pending};

        self.total_rows_pos += chunk.len();
        self.fetch_future = Some(self.make_future(con, handle));

        Poll::Ready(Some(Ok(Either::Left(chunk))))
    }
}

impl<'a> Stream for ChunkStream<'a> {
    type Item = Result<Either<DataChunk, &'a mut ExaConnection>, String>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let Some(future) = self.fetch_future.take() else {return Poll::Ready(None)};

        match future {
            ChunkFuture::Next((h, f)) => self.poll_and_process_future(f, h, cx),
            ChunkFuture::End(con) => Poll::Ready(Some(Ok(Either::Right(con)))),
        }
    }
}

enum ChunkFuture<'a> {
    Next((u16, NextChunkFuture<'a>)),
    End(&'a mut ExaConnection),
}

impl<'a> ChunkFuture<'a> {
    fn new_next_chunk(
        con: &'a mut ExaConnection,
        handle: u16,
        pos: usize,
        num_bytes: usize,
    ) -> Self {
        let cmd = Fetch::new(handle, pos, num_bytes);
        let fut = async move { con.fetch_chunk(cmd).await.map(|chunk| (chunk, con)) };
        Self::Next((handle, Box::pin(fut)))
    }
}

struct ChunkIter {
    columns: Arc<[ExaColumn]>,
    chunk_rows_total: usize,
    chunk_rows_pos: usize,
    data: Arc<[Vec<Value>]>,
}

impl ChunkIter {
    fn renew(&mut self, chunk: DataChunk) {
        self.chunk_rows_pos = 0;
        self.chunk_rows_total = chunk.chunk_rows_num;
        self.data = chunk.data.into()
    }
}

impl Iterator for ChunkIter {
    type Item = ExaRow;

    fn next(&mut self) -> Option<Self::Item> {
        if self.chunk_rows_pos >= self.chunk_rows_total {
            return None;
        }

        let row = ExaRow::new(self.data.clone(), self.columns.clone(), self.chunk_rows_pos);
        self.chunk_rows_pos += 1;
        Some(row)
    }
}

fn wake_and_pend<T>(cx: &mut Context<'_>) -> Poll<T> {
    cx.waker().wake_by_ref();
    Poll::Pending
}
