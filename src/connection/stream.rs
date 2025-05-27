//! This module contains the logic for streaming database rows from a [`ResultSet`]
//! returned from a query execution.
//!
//! It contains lower level streams and futures as the process felt too convoluted
//! to simply be handled by something like [`futures_util::stream::unfold`].
//!
//! While intimidating at first, the types in the module are easier to follow due to their implicit
//! hierarchy.

use std::{
    future::Ready,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    vec,
};

use futures_core::ready;
use futures_util::{
    stream::{self, Once},
    Future, SinkExt, Stream, StreamExt,
};
use pin_project::pin_project;
use serde_json::Value;
use sqlx_core::{logger::QueryLogger, Either, Error as SqlxError, HashMap};

use crate::{
    column::ExaColumn,
    command::ExaCommand,
    connection::websocket::ExaWebSocket,
    query_result::ExaQueryResult,
    responses::{DataChunk, ExaResult, QueryResult, ResultSet, ResultSetOutput},
    row::ExaRow,
};

/// Adapter stream that stores a future following the query execution
/// and then a stream of results from the database.
///
/// This is the top of the hierarchy and the actual type used to stream rows
/// from a [`ResultSet`].
#[pin_project]
pub struct ResultStream<'a, F>
where
    F: Future<Output = Result<QueryResultStream<'a>, SqlxError>>,
{
    #[pin]
    state: ResultStreamState<'a, F>,
    logger: QueryLogger<'a>,
    had_err: bool,
}

impl<'a, F> ResultStream<'a, F>
where
    F: Future<Output = Result<QueryResultStream<'a>, SqlxError>>,
{
    pub fn new(future: F, logger: QueryLogger<'a>) -> Self {
        Self {
            state: ResultStreamState::Initial(future),
            had_err: false,
            logger,
        }
    }

    /// Helper method that performs the actual polling on the underlying state.
    fn poll_next_impl(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<<Self as Stream>::Item>> {
        loop {
            let mut this = self.as_mut().project();
            let state = this.state.as_mut().project();
            return match state {
                ResultStreamStateProj::Stream(stream) => stream.poll_next_unpin(cx),
                ResultStreamStateProj::Initial(fut) => {
                    let stream = ready!(fut.poll(cx)?);
                    this.state.set(ResultStreamState::Stream(stream));
                    continue;
                }
            };
        }
    }
}

/// The [`Stream`] implementation here encapsulates end-stages actions
/// such as using the [`QueryLogger`] or taking note of whether an error occurred
/// (and stop streaming if it did).
impl<'a, F> Stream for ResultStream<'a, F>
where
    F: Future<Output = Result<QueryResultStream<'a>, SqlxError>>,
{
    type Item = Result<Either<ExaQueryResult, ExaRow>, SqlxError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.had_err {
            return Poll::Ready(None);
        }

        let Some(res) = ready!(self.as_mut().poll_next_impl(cx)) else {
            return Poll::Ready(None);
        };

        let this = self.project();

        match &res {
            Ok(Either::Left(q)) => this.logger.increase_rows_affected(q.rows_affected()),
            Ok(Either::Right(_)) => this.logger.increment_rows_returned(),
            Err(_) => *this.had_err = true,
        }

        Poll::Ready(Some(res))
    }
}

/// State used to distinguish between the initial query execution
/// and the subsequent streaming of rows.
#[pin_project(project = ResultStreamStateProj)]
enum ResultStreamState<'a, F>
where
    F: Future<Output = Result<QueryResultStream<'a>, SqlxError>>,
{
    Initial(#[pin] F),
    Stream(QueryResultStream<'a>),
}

/// A stream over either a result set or a single element stream
/// containing the count of affected rows.
///
/// This completely encapsulates the result streaming, but
/// does not handle all edge actions such as stopping if an error occurs.
pub enum QueryResultStream<'a> {
    ResultSet(ResultSetStream<'a>),
    RowCount(Once<Ready<Result<ExaQueryResult, SqlxError>>>),
}

impl<'a> QueryResultStream<'a> {
    pub fn new(ws: &'a mut ExaWebSocket, query_result: QueryResult) -> Result<Self, SqlxError> {
        let stream = match query_result {
            QueryResult::ResultSet { result_set: rs } => Self::result_set(rs, ws)?,
            QueryResult::RowCount { row_count } => Self::row_count(row_count),
        };

        Ok(stream)
    }

    fn row_count(row_count: u64) -> Self {
        let output = ExaQueryResult::new(row_count);
        let stream = stream::once(std::future::ready(Ok(output)));
        Self::RowCount(stream)
    }

    fn result_set(rs: ResultSet, ws: &'a mut ExaWebSocket) -> Result<Self, SqlxError> {
        let stream = ResultSetStream::new(rs, ws)?;
        Ok(Self::ResultSet(stream))
    }
}

impl<'a> Stream for QueryResultStream<'a> {
    type Item = Result<Either<ExaQueryResult, ExaRow>, SqlxError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.get_mut() {
            QueryResultStream::ResultSet(rs) => rs
                .poll_next_unpin(cx)
                .map(|o| o.map(|r| r.map(Either::Right))),
            QueryResultStream::RowCount(rc) => rc
                .poll_next_unpin(cx)
                .map(|o| o.map(|r| r.map(Either::Left))),
        }
    }
}

/// A stream over an entire result set.
/// The stream will fetch the data chunks one by one,
/// while repopulating the chunk iterator, which then gets
/// iterated over to output rows.
///
/// This encapsulates the actual database row streaming from a [`ResultSet`].
pub struct ResultSetStream<'a> {
    chunk_stream: ChunkStream<'a>,
    chunk_iter: ChunkIter,
}

impl<'a> ResultSetStream<'a> {
    fn new(rs: ResultSet, ws: &'a mut ExaWebSocket) -> Result<Self, SqlxError> {
        let ResultSet {
            total_rows_num,
            total_rows_pos,
            output,
            columns,
        } = rs;

        let chunk_stream = match output {
            ResultSetOutput::Handle(handle) => {
                let chunk_stream = MultiChunkStream {
                    ws,
                    handle,
                    total_rows_num,
                    total_rows_pos,
                    state: StreamState::Sending,
                };

                ChunkStream::Multi(chunk_stream)
            }
            ResultSetOutput::Data(data) => {
                let num_rows = total_rows_pos;
                let data_chunk = DataChunk { num_rows, data };
                let chunk_stream = stream::once(std::future::ready(Ok(data_chunk)));
                ChunkStream::Single(chunk_stream)
            }
        };

        let stream = Self {
            chunk_iter: ChunkIter::new(columns),
            chunk_stream,
        };

        Ok(stream)
    }
}

impl<'a> Stream for ResultSetStream<'a> {
    type Item = Result<ExaRow, SqlxError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(row) = self.chunk_iter.next() {
                return Poll::Ready(Some(Ok(row)));
            }

            match ready!(self.chunk_stream.poll_next_unpin(cx)?) {
                Some(chunk) => self.chunk_iter.renew(chunk),
                None => return Poll::Ready(None),
            }
        }
    }
}

/// Enum keeping track of the data chunks we expect from the server.
/// If there are less than 1000 rows in the result set, Exasol directly sends them.
/// Hence, we have a single chunk stream variant.
///
/// If there are more, we need to fetch chunks one by one using a [`MultiChunkStream`].
///
/// The purpose of this stream is to retrieve row chunks from the database
/// so that they can be iterated over and returned by the higher level streams.
enum ChunkStream<'a> {
    Multi(MultiChunkStream<'a>),
    Single(Once<Ready<Result<DataChunk, SqlxError>>>),
}

impl<'a> Stream for ChunkStream<'a> {
    type Item = Result<DataChunk, SqlxError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.get_mut() {
            Self::Multi(s) => s.poll_next_unpin(cx),
            Self::Single(s) => s.poll_next_unpin(cx),
        }
    }
}

/// A stream of chunks for a given result set that was given a handle.
///
/// This is used if the [`ResultSet`] has more than 1000 rows, so data will
/// arrive in chunks of rows which can then be iterated over.
struct MultiChunkStream<'a> {
    ws: &'a mut ExaWebSocket,
    handle: u16,
    total_rows_num: usize,
    total_rows_pos: usize,
    state: StreamState,
}

impl<'a> Stream for MultiChunkStream<'a> {
    type Item = Result<DataChunk, SqlxError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.state {
                StreamState::Sending => {
                    if self.total_rows_pos >= self.total_rows_num {
                        return Poll::Ready(None);
                    }

                    ready!(self.ws.poll_ready_unpin(cx))?;

                    let fetch_size = self.ws.attributes.fetch_size();
                    let cmd = ExaCommand::new_fetch(self.handle, self.total_rows_pos, fetch_size);
                    self.ws.start_send_unpin(cmd.try_into()?)?;

                    self.state = StreamState::Receiving;
                }
                StreamState::Receiving => {
                    let Some(res) = ready!(self.ws.poll_next_unpin(cx)) else {
                        return Poll::Ready(None);
                    };

                    let bytes = res?;
                    let (chunk, attr_opt) = Result::from(ExaResult::<DataChunk>::try_from(bytes)?)?;

                    if let Some(attributes) = attr_opt {
                        self.ws.attributes.update(attributes);
                    }

                    self.total_rows_pos += chunk.num_rows;
                    self.state = StreamState::Sending;

                    return Poll::Ready(Some(Ok(chunk)));
                }
            }
        }
    }
}

enum StreamState {
    Sending,
    Receiving,
}

/// An iterator over a chunk of data from a result set.
///
/// This is the lowest level of the streaming hierarchy and
/// merely iterates over an already retrieved chunk of rows,
/// not dealing at all with async operations.
struct ChunkIter {
    column_names: Arc<HashMap<Arc<str>, usize>>,
    columns: Arc<[ExaColumn]>,
    chunk_rows_total: usize,
    chunk_rows_pos: usize,
    data: vec::IntoIter<Vec<Value>>,
}

impl ChunkIter {
    fn new(columns: Arc<[ExaColumn]>) -> Self {
        let column_names = columns
            .iter()
            .enumerate()
            .map(|(i, c)| (c.name.clone(), i))
            .collect();

        Self {
            column_names: Arc::new(column_names),
            columns,
            chunk_rows_total: 0,
            chunk_rows_pos: 0,
            data: Vec::new().into_iter(),
        }
    }
}

impl ChunkIter {
    fn renew(&mut self, chunk: DataChunk) {
        self.chunk_rows_pos = 0;
        self.chunk_rows_total = chunk.num_rows;
        self.data = chunk.data.into_iter();
    }
}

impl Iterator for ChunkIter {
    type Item = ExaRow;

    fn next(&mut self) -> Option<Self::Item> {
        debug_assert!(self.chunk_rows_pos <= self.chunk_rows_total);

        let row = ExaRow::new(
            self.data.next()?,
            self.columns.clone(),
            self.column_names.clone(),
        );

        self.chunk_rows_pos += 1;
        Some(row)
    }
}
