use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use either::Either;
use futures_util::{
    future::{self, BoxFuture},
    stream::{self, BoxStream},
    FutureExt, Stream,
};
use serde::Deserialize;
use serde_json::Value;

use crate::{
    column::ExaColumn,
    command::{CloseResultSet, Fetch},
    connection::ExaConnection,
    query_result::ExaQueryResult,
    row::ExaRow,
};

use super::fetched::DataChunk;

type NextChunkFuture<'a> = BoxFuture<'a, Result<(DataChunk, &'a mut ExaConnection), String>>;
type ReturnConFuture<'a> = BoxFuture<'a, Result<&'a mut ExaConnection, String>>;

const NUM_BYTES: usize = 5 * 1024 * 1024;

/// Struct used for deserialization of the JSON
/// returned after executing one or more queries
/// Represents the collection of results from all queries.
#[allow(unused)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Results {
    num_results: u8,
    pub(crate) results: Vec<QueryResult>,
}

pub struct ResultsStream<'a> {
    results: std::vec::IntoIter<QueryResult>,
    current_stream: QueryResultStream<'a>,
}

impl<'a> ResultsStream<'a> {
    pub fn new(con: &'a mut ExaConnection, results: Results) -> Result<Self, String> {
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

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let stream = self.get_mut();
        let Poll::Ready(opt) = Pin::new(&mut stream.current_stream).poll_next(cx)? else {return Poll::Pending};
        let Some(either) = opt else {return Poll::Ready(None)};

        match either {
            Either::Left((qr, con)) => match stream.renew_stream(con) {
                Ok(_) => Poll::Ready(Some(Ok(Either::Left(qr)))),
                Err(e) => Poll::Ready(Some(Err(e))),
            },
            Either::Right(inner) => match inner {
                Either::Left(row) => Poll::Ready(Some(Ok(Either::Right(row)))),
                Either::Right(con) => match stream.renew_stream(con) {
                    Ok(_) => {
                        // We updated the stream so we want
                        // to be polled again after this call ends
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                    Err(e) => Poll::Ready(Some(Err(e))),
                },
            },
        }
    }
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
    RowCount(BoxStream<'a, Result<(ExaQueryResult, &'a mut ExaConnection), String>>),
}

impl<'a> QueryResultStream<'a> {
    pub fn new(con: &'a mut ExaConnection, query_result: QueryResult) -> Result<Self, String> {
        match query_result {
            QueryResult::ResultSet { result_set } => {
                Ok(Self::ResultSet(ResultSetStream::new(result_set, con)?))
            }
            QueryResult::RowCount { row_count } => Ok(Self::RowCount(Box::pin(stream::once(
                future::ready(Ok((ExaQueryResult::new(row_count as u64), con))),
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
    data_chunk: DataChunk,
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

pub struct ChunkStream<'a> {
    fetch_future: Option<ChunkFuture<'a>>,
    total_rows_num: usize,
    total_rows_pos: usize,
}

enum ChunkFuture<'a> {
    NextChunk((u16, NextChunkFuture<'a>)),
    CloseResult(ReturnConFuture<'a>),
    SingleChunked(&'a mut ExaConnection),
}

impl<'a> ChunkFuture<'a> {
    async fn next_chunk(
        con: &'a mut ExaConnection,
        cmd: Fetch,
    ) -> Result<(DataChunk, &'a mut ExaConnection), String> {
        con.fetch_chunk(cmd).await.map(|chunk| (chunk, con))
    }

    async fn close_result(
        con: &'a mut ExaConnection,
        handle: u16,
    ) -> Result<&'a mut ExaConnection, String> {
        ExaConnection::close_result_set(con, CloseResultSet::new(handle))
            .await
            .map(|_| con)
    }

    fn new_next_chunk(
        con: &'a mut ExaConnection,
        handle: u16,
        pos: usize,
        num_bytes: usize,
    ) -> Self {
        let cmd = Fetch::new(handle, pos, num_bytes);
        let fut = Self::next_chunk(con, cmd);
        Self::NextChunk((handle, Box::pin(fut)))
    }

    fn new_close_result(con: &'a mut ExaConnection, handle: u16) -> Self {
        let fut = Self::close_result(con, handle);
        Self::CloseResult(Box::pin(fut))
    }
}

impl<'a> Stream for ChunkStream<'a> {
    type Item = Result<Either<DataChunk, &'a mut ExaConnection>, String>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut stream = self.get_mut();

        let Some(future) = stream.fetch_future.take() else {return Poll::Ready(None)};

        let (output, future) = match future {
            ChunkFuture::NextChunk((h, mut f)) => {
                let Poll::Ready((chunk, con)) = f.poll_unpin(cx)? else {return Poll::Pending};
                stream.total_rows_pos += chunk.len();

                let future = if stream.total_rows_pos < stream.total_rows_num {
                    ChunkFuture::new_next_chunk(con, h, stream.total_rows_pos, NUM_BYTES)
                } else {
                    ChunkFuture::new_close_result(con, h)
                };

                (Poll::Ready(Some(Ok(Either::Left(chunk)))), Some(future))
            }

            ChunkFuture::CloseResult(mut f) => {
                let Poll::Ready(con) = f.poll_unpin(cx)? else {return Poll::Pending};
                (Poll::Ready(Some(Ok(Either::Right(con)))), None)
            }

            ChunkFuture::SingleChunked(con) => (Poll::Ready(Some(Ok(Either::Right(con)))), None),
        };

        stream.fetch_future = future;
        output
    }
}

// #[derive(Debug)]
pub struct ResultSetStream<'a> {
    err_encountered: bool,
    chunk_iter: ChunkIter,
    chunk_stream: ChunkStream<'a>,
}

impl<'a> ResultSetStream<'a> {
    pub(crate) fn new(rs: ResultSet, con: &'a mut ExaConnection) -> Result<Self, String> {
        let pos = rs.data_chunk.len();

        let fetch_future = if pos < rs.total_rows_num {
            let handle = rs
                .result_set_handle
                .ok_or_else(|| "Missing result set handle".to_owned())?;

            Some(ChunkFuture::new_next_chunk(con, handle, pos, NUM_BYTES))
        } else {
            Some(ChunkFuture::SingleChunked(con))
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
            err_encountered: false,
            chunk_iter,
            chunk_stream,
        };

        Ok(stream)
    }
}

impl<'a> Stream for ResultSetStream<'a> {
    type Item = Result<Either<ExaRow, &'a mut ExaConnection>, String>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let stream = self.get_mut();

        if stream.err_encountered {
            return Poll::Ready(None);
        }

        if let Some(row) = stream.chunk_iter.next() {
            return Poll::Ready(Some(Ok(Either::Left(row))));
        }

        let Poll::Ready(opt_res) = Pin::new(&mut stream.chunk_stream).poll_next(cx) else {return Poll::Pending};
        let Some(res) = opt_res else {return Poll::Ready(None)};

        match res {
            Err(e) => {
                stream.err_encountered = true;
                Poll::Ready(Some(Err(e)))
            }
            Ok(either) => match either {
                Either::Left(chunk) => {
                    // We updated the iterator so we want
                    // to be polled again after this call ends
                    stream.chunk_iter.renew(chunk);
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                Either::Right(con) => Poll::Ready(Some(Ok(Either::Right(con)))),
            },
        }
    }
}
