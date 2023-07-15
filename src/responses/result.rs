use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use either::Either;
use futures_util::{
    future::{self, BoxFuture, Ready},
    stream::{self, Once},
    FutureExt, Stream,
};
use serde::Deserialize;
use serde_json::Value;

use crate::{
    column::ExaColumn,
    command::{Command, Fetch},
    connection::ExaConnection,
    query_result::ExaQueryResult,
    row::ExaRow,
};

use super::fetched::DataChunk;

type NextChunkFuture<'a> = BoxFuture<'a, Result<(DataChunk, &'a mut ExaConnection), String>>;
type GetResultsFuture<'a> = BoxFuture<'a, Result<ResultsStream<'a>, String>>;

const NUM_BYTES: usize = 5 * 1024 * 1024;

pub struct StopOnErrorStream<'a> {
    inner: SomeStream<'a>,
    stop: bool,
}

impl<'a> StopOnErrorStream<'a> {
    pub fn new(con: &'a mut ExaConnection, command: Command) -> Self {
        Self {
            inner: SomeStream::new(con, command),
            stop: false,
        }
    }
}

impl<'a> Stream for StopOnErrorStream<'a> {
    type Item = Result<Either<ExaQueryResult, ExaRow>, sqlx::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.stop {
            Poll::Ready(None)
        } else {
            let Poll::Ready(opt_res) = Pin::new(&mut self.inner).poll_next(cx) else {return Poll::Pending};
            let Some(res) = opt_res else { return Poll::Ready(None)} ;
            self.stop = res.is_err();
            Poll::Ready(Some(res.map_err(sqlx::Error::Protocol)))
        }
    }
}

pub struct SomeStream<'a>(SomeStreamState<'a>);

enum SomeStreamState<'a> {
    Initial(GetResultsFuture<'a>),
    Stream(ResultsStream<'a>),
}

impl<'a> SomeStream<'a> {
    pub fn new(con: &'a mut ExaConnection, command: Command) -> Self {
        let fut = Box::pin(con.get_results_stream(command));
        Self(SomeStreamState::Initial(fut))
    }
}

impl<'a> Stream for SomeStream<'a> {
    type Item = Result<Either<ExaQueryResult, ExaRow>, String>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut self.0 {
            SomeStreamState::Initial(fut) => {
                let Poll::Ready(stream) = fut.poll_unpin(cx)? else {return Poll::Pending};
                self.0 = SomeStreamState::Stream(stream);
                Poll::Pending
            }
            SomeStreamState::Stream(stream) => Pin::new(stream).poll_next(cx),
        }
    }
}

/// Struct used for deserialization of the JSON
/// returned after executing one or more queries
/// Represents the collection of results from all queries.
#[allow(unused)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Results {
    num_results: u8,
    results: Vec<QueryResult>,
}

impl Results {
    fn handles(&self) -> Option<Vec<u16>> {
        self.results.iter().map(|qr| qr.handle()).collect()
    }
}

pub struct ResultsStream<'a> {
    results: std::vec::IntoIter<QueryResult>,
    current_stream: QueryResultStream<'a>,
}

impl<'a> ResultsStream<'a> {
    pub fn new(con: &'a mut ExaConnection, results: Results) -> Result<Self, String> {
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
}

impl<'a> Stream for ResultsStream<'a> {
    type Item = Result<Either<ExaQueryResult, ExaRow>, String>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let Poll::Ready(opt) = Pin::new(&mut self.current_stream).poll_next(cx)? else {return Poll::Pending};
        let Some(either) = opt else {return Poll::Ready(None)};

        let res = match either {
            Either::Left((qr, con)) => self.renew_stream(con).map(|_| Either::Left(qr)),
            Either::Right(inner) => match inner {
                Either::Left(row) => Ok(Either::Right(row)),
                Either::Right(con) => match self.renew_stream(con) {
                    Ok(_) => {
                        // We updated the stream so we want
                        // to be polled again after this call ends
                        cx.waker().wake_by_ref();
                        return Poll::Pending;
                    }
                    Err(e) => Err(e),
                },
            },
        };

        Poll::Ready(Some(res))
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
    pub fn new(con: &'a mut ExaConnection, query_result: QueryResult) -> Result<Self, String> {
        match query_result {
            QueryResult::ResultSet { result_set } => {
                Ok(Self::ResultSet(ResultSetStream::new(result_set, con)?))
            }
            QueryResult::RowCount { row_count } => Ok(Self::RowCount(stream::once(future::ready(
                Ok((ExaQueryResult::new(row_count), con)),
            )))),
        }
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
    num_columns: usize,
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
            num_columns: rs.num_columns,
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
            Either::Left(chunk) => {
                // We updated the iterator so we want
                // to be polled again after this call ends
                self.chunk_iter.renew(chunk);
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Either::Right(con) => Poll::Ready(Some(Ok(Either::Right(con)))),
        }
    }
}

pub struct ChunkStream<'a> {
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

impl<'a> Stream for ChunkStream<'a> {
    type Item = Result<Either<DataChunk, &'a mut ExaConnection>, String>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let Some(future) = self.fetch_future.take() else {return Poll::Ready(None)};

        match future {
            ChunkFuture::Next((h, mut f)) => {
                let Poll::Ready((chunk, con)) = f.poll_unpin(cx)? else {return Poll::Pending};

                self.total_rows_pos += chunk.len();
                self.fetch_future = Some(self.make_future(con, h));

                Poll::Ready(Some(Ok(Either::Left(chunk))))
            }

            ChunkFuture::End(con) => Poll::Ready(Some(Ok(Either::Right(con)))),
        }
    }
}

struct ChunkIter {
    num_columns: usize,
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
