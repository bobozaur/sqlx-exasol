use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use either::Either;
use futures_util::{
    future::{self, BoxFuture},
    stream::{self, BoxStream},
    Stream,
};
use serde::Deserialize;
use serde_json::Value;

use crate::{
    column::ExaColumn, command::Fetch, connection::ExaConnection, query_result::ExaQueryResult,
    row::ExaRow,
};

use super::fetched::FetchedData;

type FetchFuture<'a> = BoxFuture<'a, Result<(FetchedData, &'a mut ExaConnection), String>>;

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

pub struct ResultsStream<'a, I>
where
    I: Iterator<Item = QueryResult>,
{
    results: I,
    stream: BoxStream<'a, Result<Either<ExaQueryResult, ExaRow>, String>>,
}

/// Struct used for deserialization of the JSON
/// returned sending queries to the database.
/// Represents the result of one query.
#[allow(non_snake_case)]
#[derive(Debug, Deserialize)]
#[serde(tag = "resultType", rename_all = "camelCase")]
pub enum QueryResult {
    #[serde(rename_all = "camelCase")]
    ResultSet { result_set: ResultSet },
    #[serde(rename_all = "camelCase")]
    RowCount { row_count: usize },
}

pub enum QueryResultStream<'a> {
    ResultSet(ResultSetStream<'a>),
    RowCount(BoxStream<'a, Result<ExaQueryResult, String>>),
}

impl<'a> QueryResultStream<'a> {
    pub fn new(connection: &'a mut ExaConnection, query_result: QueryResult) -> Self {
        match query_result {
            QueryResult::ResultSet { result_set } => {
                Self::ResultSet(ResultSetStream::new(result_set, connection))
            }
            QueryResult::RowCount { row_count } => Self::RowCount(Box::pin(stream::once(
                future::ready(Ok(ExaQueryResult::new(row_count as u64))),
            ))),
        }
    }
}

impl<'a> Stream for QueryResultStream<'a> {
    type Item = Result<Either<ExaQueryResult, ExaRow>, String>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.get_mut() {
            QueryResultStream::RowCount(rc) => Pin::new(rc)
                .poll_next(cx)
                .map(|o| o.map(|r| r.map(Either::Left))),
            QueryResultStream::ResultSet(rs) => Pin::new(rs)
                .poll_next(cx)
                .map(|o| o.map(|r| r.map(Either::Right))),
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
    fetched: FetchedData,
}

struct ChunkIter {
    num_columns: usize,
    columns: Arc<[ExaColumn]>,
    chunk_rows_total: usize,
    chunk_rows_pos: usize,
    data: Arc<[Vec<Value>]>,
}

impl ChunkIter {
    fn renew(&mut self, chunk: FetchedData) {
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

pub struct ChunkStream<'a> {
    fetch_future: FetchFuture<'a>,
    rs_handle: Option<u16>,
    total_rows_num: usize,
    total_rows_pos: usize,
}

impl<'a> ChunkStream<'a> {
    async fn fetch_chunk(
        con: &'a mut ExaConnection,
        handle: Option<u16>,
        pos: usize,
        num_bytes: usize,
    ) -> Result<(FetchedData, &'a mut ExaConnection), String> {
        let result_set_handle = handle.ok_or_else(|| "No result set handle present".to_owned())?;
        let cmd = Fetch::new(result_set_handle, pos, num_bytes);
        con.fetch_chunk(cmd).await.map(|f| (f, con))
    }

    fn make_future(con: &'a mut ExaConnection, handle: Option<u16>, pos: usize) -> FetchFuture<'a> {
        Box::pin(ChunkStream::fetch_chunk(con, handle, pos, 5 * 1024 * 1024))
    }

    fn set_future(&mut self, con: &'a mut ExaConnection, rows_retrieved: usize) {
        self.total_rows_pos += rows_retrieved;
        self.fetch_future = Self::make_future(con, self.rs_handle, self.total_rows_pos);
    }
}

impl<'a> Stream for ChunkStream<'a> {
    type Item = Result<FetchedData, String>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let stream = self.get_mut();

        if stream.total_rows_pos >= stream.total_rows_num {
            // TODO: Maybe close the result set from here if it was fully retrieved?
            //       It is now closed from the connection when executing a new query
            return Poll::Ready(None);
        }

        let Poll::Ready(res) = stream.fetch_future.as_mut().poll(cx) else {return Poll::Pending};

        Poll::Ready(Some(res.map(|(f, c)| {
            stream.set_future(c, f.len());
            f
        })))
    }
}

// #[derive(Debug)]
pub struct ResultSetStream<'a> {
    err_encountered: bool,
    chunk_iter: ChunkIter,
    data_stream: ChunkStream<'a>,
}

impl<'a> ResultSetStream<'a> {
    pub(crate) fn new(rs: ResultSet, con: &'a mut ExaConnection) -> Self {
        let pos = rs.fetched.len();
        let fetch_future = ChunkStream::make_future(con, rs.result_set_handle, pos);

        let data_stream = ChunkStream {
            rs_handle: rs.result_set_handle,
            total_rows_num: rs.total_rows_num,
            total_rows_pos: pos,
            fetch_future,
        };

        let chunk_iter = ChunkIter {
            num_columns: rs.num_columns,
            columns: rs.columns.into(),
            chunk_rows_total: rs.fetched.chunk_rows_num,
            chunk_rows_pos: 0,
            data: rs.fetched.data.into(),
        };

        Self {
            err_encountered: false,
            chunk_iter,
            data_stream,
        }
    }
}

impl<'a> Stream for ResultSetStream<'a> {
    type Item = Result<ExaRow, String>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let stream = self.get_mut();

        if stream.err_encountered {
            return Poll::Ready(None);
        }

        if let Some(row) = stream.chunk_iter.next() {
            return Poll::Ready(Some(Ok(row)));
        }

        let Poll::Ready(opt_res) = Pin::new(&mut stream.data_stream).poll_next(cx) else {return std::task::Poll::Pending};
        let Some(res) = opt_res else {return Poll::Ready(None)};

        if res.is_err() {
            stream.err_encountered = true;
        }

        match res {
            Err(e) => Poll::Ready(Some(Err(e))),
            Ok(chunk) => {
                stream.chunk_iter.renew(chunk);
                Poll::Ready(stream.chunk_iter.next().map(Ok))
            }
        }
    }
}
