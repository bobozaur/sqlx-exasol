use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use either::Either;
use futures_core::ready;
use futures_util::{
    future::{self, Ready},
    pin_mut,
    stream::{self, Once},
    Future, Stream,
};
use pin_project::pin_project;

use serde_json::Value;
use sqlx_core::{logger::QueryLogger, Error as SqlxError, HashMap};

use crate::{
    column::ExaColumn,
    command::{Command, ExaCommand},
    connection::websocket::ExaWebSocket,
    error::ExaProtocolError,
    query_result::ExaQueryResult,
    responses::{DataChunk, QueryResult, ResultSet},
    row::ExaRow,
};

/// Adapter stream that stores a future following the query execution
/// and then a stream of results from the database.
#[pin_project]
pub struct ResultStream<'a, C, F1, F2>
where
    C: FnMut(&'a mut ExaWebSocket, Command) -> F1,
    F1: Future<Output = Result<(DataChunk, &'a mut ExaWebSocket), SqlxError>> + 'a,
    F2: Future<Output = Result<QueryResultStream<'a, C, F1>, SqlxError>>,
{
    #[pin]
    state: ResultStreamState<'a, C, F1, F2>,
    logger: QueryLogger<'a>,
    had_err: bool,
}

impl<'a, C, F1, F2> ResultStream<'a, C, F1, F2>
where
    C: FnMut(&'a mut ExaWebSocket, Command) -> F1,
    F1: Future<Output = Result<(DataChunk, &'a mut ExaWebSocket), SqlxError>> + 'a,
    F2: Future<Output = Result<QueryResultStream<'a, C, F1>, SqlxError>>,
{
    pub fn new(future: F2, logger: QueryLogger<'a>) -> Self {
        Self {
            state: ResultStreamState::Initial(future),
            had_err: false,
            logger,
        }
    }

    fn poll_next_impl(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<<Self as Stream>::Item>> {
        let mut this = self.as_mut().project();
        let state = this.state.as_mut().project();

        match state {
            ResultStreamStateProj::Stream(stream) => stream.poll_next(cx),
            ResultStreamStateProj::Initial(fut) => {
                let stream = ready!(fut.poll(cx)?);
                this.state.set(ResultStreamState::Stream(stream));

                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}

impl<'a, C, F1, F2> Stream for ResultStream<'a, C, F1, F2>
where
    C: FnMut(&'a mut ExaWebSocket, Command) -> F1,
    F1: Future<Output = Result<(DataChunk, &'a mut ExaWebSocket), SqlxError>> + 'a,
    F2: Future<Output = Result<QueryResultStream<'a, C, F1>, SqlxError>>,
{
    type Item = Result<Either<ExaQueryResult, ExaRow>, SqlxError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.had_err {
            return Poll::Ready(None);
        }

        let opt = ready!(self.as_mut().poll_next_impl(cx));
        let Some(res) = opt else {
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

// State used to distinguish between the initial query execution
// and the subsequent streaming of rows.
#[pin_project(project = ResultStreamStateProj)]
enum ResultStreamState<'a, C, F1, F2>
where
    C: FnMut(&'a mut ExaWebSocket, Command) -> F1,
    F1: Future<Output = Result<(DataChunk, &'a mut ExaWebSocket), SqlxError>> + 'a,
    F2: Future<Output = Result<QueryResultStream<'a, C, F1>, SqlxError>>,
{
    Initial(#[pin] F2),
    Stream(#[pin] QueryResultStream<'a, C, F1>),
}

/// A stream over either a result set or a single element stream
/// containing the count of affected rows.
#[pin_project(project = QueryResultStreamProj)]
pub enum QueryResultStream<'a, C, F>
where
    C: FnMut(&'a mut ExaWebSocket, Command) -> F,
    F: Future<Output = Result<(DataChunk, &'a mut ExaWebSocket), SqlxError>> + 'a,
{
    ResultSet(#[pin] ResultSetStream<'a, C, F>),
    RowCount(#[pin] Once<Ready<Result<ExaQueryResult, SqlxError>>>),
}

impl<'a, C, F> QueryResultStream<'a, C, F>
where
    C: FnMut(&'a mut ExaWebSocket, Command) -> F,
    F: Future<Output = Result<(DataChunk, &'a mut ExaWebSocket), SqlxError>> + 'a,
{
    pub fn new(
        ws: &'a mut ExaWebSocket,
        query_result: QueryResult,
        fetcher_maker: C,
    ) -> Result<Self, SqlxError> {
        let stream = match query_result {
            QueryResult::ResultSet { result_set: rs } => Self::result_set(rs, ws, fetcher_maker)?,
            QueryResult::RowCount { row_count } => Self::row_count(row_count),
        };

        Ok(stream)
    }

    fn row_count(row_count: u64) -> Self {
        let output = ExaQueryResult::new(row_count);
        let stream = stream::once(future::ready(Ok(output)));
        Self::RowCount(stream)
    }

    fn result_set(
        rs: ResultSet,
        ws: &'a mut ExaWebSocket,
        fetcher_maker: C,
    ) -> Result<Self, SqlxError> {
        let stream = ResultSetStream::new(rs, ws, fetcher_maker)?;
        Ok(Self::ResultSet(stream))
    }
}

impl<'a, C, F> Stream for QueryResultStream<'a, C, F>
where
    C: FnMut(&'a mut ExaWebSocket, Command) -> F,
    F: Future<Output = Result<(DataChunk, &'a mut ExaWebSocket), SqlxError>> + 'a,
{
    type Item = Result<Either<ExaQueryResult, ExaRow>, SqlxError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        macro_rules! poll_map {
            ($val:expr, $var:ident) => {
                $val.poll_next(cx).map(|o| o.map(|r| r.map(Either::$var)))
            };
        }

        match self.project() {
            QueryResultStreamProj::ResultSet(rs) => poll_map!(rs, Right),
            QueryResultStreamProj::RowCount(rc) => poll_map!(rc, Left),
        }
    }
}

/// A stream over an entire result set.
/// All result sets are sent by Exasol with the first data chunk.
/// If there will be more, the stream will fetch them one by one,
/// while repopulating the chunk iterator, which then gets
/// iterated over to output rows.
#[pin_project]
pub struct ResultSetStream<'a, C, F>
where
    C: FnMut(&'a mut ExaWebSocket, Command) -> F,
    F: Future<Output = Result<(DataChunk, &'a mut ExaWebSocket), SqlxError>> + 'a,
{
    chunk_iter: ChunkIter,
    #[pin]
    chunk_stream: ChunkStream<'a, C, F>,
}

impl<'a, C, F> ResultSetStream<'a, C, F>
where
    C: FnMut(&'a mut ExaWebSocket, Command) -> F,
    F: Future<Output = Result<(DataChunk, &'a mut ExaWebSocket), SqlxError>> + 'a,
{
    fn new(
        rs: ResultSet,
        ws: &'a mut ExaWebSocket,
        mut fetcher_maker: C,
    ) -> Result<Self, SqlxError> {
        let pos = rs.data_chunk.num_rows;

        let fetcher_parts = if pos < rs.total_rows_num {
            let handle = rs
                .result_set_handle
                .ok_or(ExaProtocolError::MissingResultHandle)?;

            let cmd = ExaCommand::new_fetch(handle, pos, ws.attributes.fetch_size).try_into()?;
            let future = fetcher_maker(ws, cmd);
            Some((handle, future))
        } else {
            None
        };

        let chunk_stream = ChunkStream {
            fetcher_maker,
            total_rows_num: rs.total_rows_num,
            total_rows_pos: pos,
            fetcher_parts,
        };

        let column_names = rs
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| (c.name.clone(), i))
            .collect();

        let chunk_iter = ChunkIter {
            column_names: Arc::new(column_names),
            columns: rs.columns,
            chunk_rows_total: rs.data_chunk.num_rows,
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

impl<'a, C, F> Stream for ResultSetStream<'a, C, F>
where
    C: FnMut(&'a mut ExaWebSocket, Command) -> F,
    F: Future<Output = Result<(DataChunk, &'a mut ExaWebSocket), SqlxError>> + 'a,
{
    type Item = Result<ExaRow, SqlxError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().project();

        if let Some(row) = this.chunk_iter.next() {
            return Poll::Ready(Some(Ok(row)));
        }

        let Poll::Ready(opt) = this.chunk_stream.poll_next(cx)? else {
            return Poll::Pending;
        };
        let Some(chunk) = opt else {
            return Poll::Ready(None);
        };

        self.chunk_iter.renew(chunk);
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

/// A stream of chunks for a given result set.
/// The chunks then get iterated over through [`ChunkIter`].
#[pin_project]
struct ChunkStream<'a, C, F>
where
    C: FnMut(&'a mut ExaWebSocket, Command) -> F,
    F: Future<Output = Result<(DataChunk, &'a mut ExaWebSocket), SqlxError>> + 'a,
{
    fetcher_maker: C,
    fetcher_parts: Option<(u16, F)>,
    total_rows_num: usize,
    total_rows_pos: usize,
}

impl<'a, C, F> ChunkStream<'a, C, F>
where
    C: FnMut(&'a mut ExaWebSocket, Command) -> F,
    F: Future<Output = Result<(DataChunk, &'a mut ExaWebSocket), SqlxError>> + 'a,
{
    fn make_fetcher(
        &mut self,
        ws: &'a mut ExaWebSocket,
        handle: u16,
    ) -> Result<Option<(u16, F)>, SqlxError> {
        if self.total_rows_pos < self.total_rows_num {
            let cmd = ExaCommand::new_fetch(handle, self.total_rows_pos, ws.attributes.fetch_size)
                .try_into()?;
            let future = (self.fetcher_maker)(ws, cmd);
            Ok(Some((handle, future)))
        } else {
            Ok(None)
        }
    }
}

impl<'a, C, F> Stream for ChunkStream<'a, C, F>
where
    C: FnMut(&'a mut ExaWebSocket, Command) -> F,
    F: Future<Output = Result<(DataChunk, &'a mut ExaWebSocket), SqlxError>> + 'a,
{
    type Item = Result<DataChunk, SqlxError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().project();
        let Some((handle, future)) = this.fetcher_parts.take() else {
            return Poll::Ready(None);
        };

        pin_mut!(future);
        let Poll::Ready((chunk, ws)) = future.poll(cx)? else {
            return Poll::Pending;
        };

        self.total_rows_pos += chunk.num_rows;
        self.fetcher_parts = self.make_fetcher(ws, handle)?;

        Poll::Ready(Some(Ok(chunk)))
    }
}

// An iterator over a chunk of data from a result set.
struct ChunkIter {
    column_names: Arc<HashMap<Arc<str>, usize>>,
    columns: Arc<[ExaColumn]>,
    chunk_rows_total: usize,
    chunk_rows_pos: usize,
    data: Arc<[Vec<Value>]>,
}

impl ChunkIter {
    fn renew(&mut self, chunk: DataChunk) {
        self.chunk_rows_pos = 0;
        self.chunk_rows_total = chunk.num_rows;
        self.data = chunk.data.into()
    }
}

impl Iterator for ChunkIter {
    type Item = ExaRow;

    fn next(&mut self) -> Option<Self::Item> {
        if self.chunk_rows_pos >= self.chunk_rows_total {
            return None;
        }

        let row = ExaRow::new(
            self.data.clone(),
            self.columns.clone(),
            self.column_names.clone(),
            self.chunk_rows_pos,
        );

        self.chunk_rows_pos += 1;
        Some(row)
    }
}