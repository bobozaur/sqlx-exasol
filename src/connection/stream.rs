//! This module contains the logic for streaming database rows from a [`ResultSet`]
//! returned from a query execution.
//!
//! It contains lower level streams and futures as the process felt too convoluted
//! to simply be handled by something like [`futures_util::stream::unfold`].
//!
//! While intimidating at first, the types in the module are easier to follow due to their implicit
//! hierarchy.

use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    vec,
};

use futures_core::ready;
use futures_util::Stream;
use serde_json::Value;
use sqlx_core::{logger::QueryLogger, Either, Error as SqlxError, HashMap};

use crate::{
    column::ExaColumn,
    connection::{
        futures::CloseResultSets,
        websocket::{
            futures::{FetchChunk, WebSocketFuture},
            ExaWebSocket,
        },
    },
    error::ExaProtocolError,
    query_result::ExaQueryResult,
    responses::{DataChunk, MultiResults, QueryResult, ResultSet, ResultSetOutput, SingleResult},
    row::ExaRow,
};

/// Adapter stream that stores a future following the query execution
/// and then a stream of results from the database.
///
/// This is the top of the hierarchy and the actual type used to stream rows
/// from a [`ResultSet`].
pub struct ResultStream<'a, F> {
    ws: &'a mut ExaWebSocket,
    logger: QueryLogger<'a>,
    result_set_handles: Vec<u16>,
    state: ResultStreamState<F>,
}

impl<'a, F> ResultStream<'a, F>
where
    F: WebSocketFuture<Output = MultiResultStream>,
{
    pub fn new(ws: &'a mut ExaWebSocket, logger: QueryLogger<'a>, future: F) -> Self {
        Self {
            ws,
            logger,
            result_set_handles: Vec::new(),
            state: ResultStreamState::Initial(future),
        }
    }
}

/// The [`Stream`] implementation here encapsulates end-stages actions
/// such as using the [`QueryLogger`] or taking note of whether an error occurred
/// (and stop streaming if it did).
impl<'a, F> Stream for ResultStream<'a, F>
where
    F: WebSocketFuture<Output = MultiResultStream>,
{
    type Item = Result<Either<ExaQueryResult, ExaRow>, SqlxError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            match &mut this.state {
                ResultStreamState::Initial(future) => {
                    let multi_stream = ready!(future.poll_unpin(cx, this.ws))?;
                    this.result_set_handles = multi_stream.handles();
                    this.state = ResultStreamState::Stream(multi_stream);
                }
                ResultStreamState::Stream(stream) => {
                    let Some(either) = ready!(stream.poll_next_unpin(cx, this.ws)).transpose()?
                    else {
                        return Poll::Ready(None);
                    };

                    match &either {
                        Either::Left(q) => this.logger.increase_rows_affected(q.rows_affected()),
                        Either::Right(_) => this.logger.increment_rows_returned(),
                    }

                    return Poll::Ready(Some(Ok(either)));
                }
            }
        }
    }
}

impl<'a, F> Drop for ResultStream<'a, F> {
    fn drop(&mut self) {
        let handles = std::mem::take(&mut self.result_set_handles);
        if !handles.is_empty() {
            self.ws.pending_close = Some(CloseResultSets::new(handles));
        }
    }
}

/// State used to distinguish between the initial query execution
/// and the subsequent streaming of rows.
enum ResultStreamState<F> {
    Initial(F),
    Stream(MultiResultStream),
}

pub struct MultiResultStream {
    next_results: vec::IntoIter<QueryResult>,
    stream: QueryResultStream,
}

impl MultiResultStream {
    pub fn new(first_result: QueryResult, next_results: vec::IntoIter<QueryResult>) -> Self {
        let stream = QueryResultStream::new(first_result);

        Self {
            next_results,
            stream,
        }
    }

    fn handles(&self) -> Vec<u16> {
        let first_handle = match &self.stream {
            QueryResultStream::RowStream(row_stream) => match &row_stream.chunk_stream {
                ChunkStream::Multi(multi_chunk_stream) => Some(multi_chunk_stream.handle),
                ChunkStream::Single(_) => None,
            },
            QueryResultStream::RowCount(_) => None,
        };

        let results_handles_iter = self
            .next_results
            .as_slice()
            .iter()
            .filter_map(QueryResult::handle);

        first_handle
            .into_iter()
            .chain(results_handles_iter)
            .collect()
    }

    fn poll_next_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<Option<Result<Either<ExaQueryResult, ExaRow>, SqlxError>>> {
        loop {
            if let Some(res) = ready!(self.stream.poll_next_unpin(cx, ws)) {
                return Poll::Ready(Some(res));
            }

            let Some(qr) = self.next_results.next() else {
                return Poll::Ready(None);
            };

            self.stream = QueryResultStream::new(qr);
        }
    }
}

impl From<SingleResult> for MultiResultStream {
    fn from(value: SingleResult) -> Self {
        let first_result = value
            .results
            .into_iter()
            .next()
            .expect("query result array must have one element");

        Self::new(first_result, Vec::new().into_iter())
    }
}

impl TryFrom<MultiResults> for MultiResultStream {
    type Error = SqlxError;

    fn try_from(value: MultiResults) -> Result<Self, Self::Error> {
        let mut next_results = value.results.into_iter();

        let Some(first_result) = next_results.next() else {
            return Err(ExaProtocolError::NoResponse)?;
        };

        Ok(MultiResultStream::new(first_result, next_results))
    }
}

/// A stream over either a result set or a single element stream
/// containing the count of affected rows.
///
/// This completely encapsulates the result streaming, but
/// does not handle all edge actions such as stopping if an error occurs.
pub enum QueryResultStream {
    RowStream(RowStream),
    RowCount(Option<ExaQueryResult>),
}

impl QueryResultStream {
    fn new(query_result: QueryResult) -> Self {
        match query_result {
            QueryResult::ResultSet { result_set: rs } => Self::RowStream(RowStream::new(rs)),
            QueryResult::RowCount { row_count } => {
                let query_result = ExaQueryResult::new(row_count);
                Self::RowCount(Some(query_result))
            }
        }
    }

    fn poll_next_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<Option<Result<Either<ExaQueryResult, ExaRow>, SqlxError>>> {
        match self {
            QueryResultStream::RowStream(rs) => rs
                .poll_next_unpin(cx, ws)
                .map(|o| o.map(|r| r.map(Either::Right))),
            QueryResultStream::RowCount(qr) => Poll::Ready(qr.take().map(Either::Left).map(Ok)),
        }
    }
}

/// A stream over the rows of a result set.
/// The stream will fetch the data chunks one by one, while repopulating the chunk iterator, which
/// then gets iterated over to output rows.
///
/// This encapsulates the actual database row streaming from a [`ResultSet`].
pub struct RowStream {
    chunk_stream: ChunkStream,
    chunk_iter: ChunkIter,
}

impl RowStream {
    fn new(rs: ResultSet) -> Self {
        let ResultSet {
            total_rows_num,
            total_rows_pos,
            output,
            columns,
        } = rs;

        let chunk_iter = ChunkIter::new(columns);

        let chunk_stream = match output {
            ResultSetOutput::Handle(handle) => {
                ChunkStream::Multi(MultiChunkStream::new(handle, total_rows_num))
            }
            ResultSetOutput::Data(data) => {
                let num_rows = total_rows_pos;
                ChunkStream::Single(Some(DataChunk { num_rows, data }))
            }
        };

        Self {
            chunk_stream,
            chunk_iter,
        }
    }
}

impl RowStream {
    fn poll_next_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<Option<Result<ExaRow, SqlxError>>> {
        loop {
            if let Some(row) = self.chunk_iter.next() {
                return Poll::Ready(Some(Ok(row)));
            }

            match ready!(self.chunk_stream.poll_next_unpin(cx, ws)?) {
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
enum ChunkStream {
    Multi(MultiChunkStream),
    Single(Option<DataChunk>),
}

impl ChunkStream {
    fn poll_next_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<Option<Result<DataChunk, SqlxError>>> {
        match self {
            Self::Multi(s) => s.poll_next_unpin(cx, ws),
            Self::Single(chunk) => Poll::Ready(chunk.take().map(Ok)),
        }
    }
}

/// A stream of chunks for a given result set that was given a handle.
///
/// This is used if the [`ResultSet`] has more than 1000 rows, so data will
/// arrive in chunks of rows which can then be iterated over.
struct MultiChunkStream {
    handle: u16,
    total_rows_num: usize,
    total_rows_pos: usize,
    state: MultiChunkStreamState,
}

impl MultiChunkStream {
    fn new(handle: u16, total_rows_num: usize) -> Self {
        Self {
            handle,
            total_rows_num,
            total_rows_pos: 0,
            state: MultiChunkStreamState::Initial,
        }
    }
}

impl MultiChunkStream {
    fn poll_next_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<Option<Result<DataChunk, SqlxError>>> {
        loop {
            match &mut self.state {
                MultiChunkStreamState::Initial => {
                    let num_bytes = ws.attributes.fetch_size();
                    let future = FetchChunk::new(self.handle, self.total_rows_pos, num_bytes);
                    self.state = MultiChunkStreamState::Polling(future);
                }
                MultiChunkStreamState::Polling(future) => {
                    if self.total_rows_pos >= self.total_rows_num {
                        self.state = MultiChunkStreamState::Finished;
                        continue;
                    }

                    let num_bytes = ws.attributes.fetch_size();
                    let chunk = ready!(future.poll_unpin(cx, ws))?;

                    self.total_rows_pos += chunk.num_rows;
                    let future = FetchChunk::new(self.handle, self.total_rows_num, num_bytes);
                    self.state = MultiChunkStreamState::Polling(future);

                    return Poll::Ready(Some(Ok(chunk)));
                }
                MultiChunkStreamState::Finished => return Poll::Ready(None),
            }
        }
    }
}

enum MultiChunkStreamState {
    Initial,
    Polling(FetchChunk),
    Finished,
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
