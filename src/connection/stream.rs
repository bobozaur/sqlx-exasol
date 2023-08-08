use std::{
    future::Ready,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use either::Either;
use futures_core::ready;
use futures_util::{
    stream::{self, Once},
    Future, Stream,
};
use pin_project::pin_project;

use serde_json::Value;
use sqlx_core::{logger::QueryLogger, Error as SqlxError, HashMap};

use crate::{
    column::ExaColumn,
    connection::websocket::ExaWebSocket,
    query_result::ExaQueryResult,
    responses::{DataChunk, QueryResult, ResultSet, ResultSetOutput},
    row::ExaRow,
};

/// Adapter stream that stores a future following the query execution
/// and then a stream of results from the database.
#[pin_project]
pub struct ResultStream<'a, C, F1, F2>
where
    C: Fn(&'a mut ExaWebSocket, u16, usize) -> Result<F1, SqlxError>,
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
    C: Fn(&'a mut ExaWebSocket, u16, usize) -> Result<F1, SqlxError>,
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
    C: Fn(&'a mut ExaWebSocket, u16, usize) -> Result<F1, SqlxError>,
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
    C: Fn(&'a mut ExaWebSocket, u16, usize) -> Result<F1, SqlxError>,
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
    C: Fn(&'a mut ExaWebSocket, u16, usize) -> Result<F, SqlxError>,
    F: Future<Output = Result<(DataChunk, &'a mut ExaWebSocket), SqlxError>> + 'a,
{
    ResultSet(#[pin] ResultSetStream<'a, C, F>),
    RowCount(#[pin] Once<Ready<Result<ExaQueryResult, SqlxError>>>),
}

impl<'a, C, F> QueryResultStream<'a, C, F>
where
    C: Fn(&'a mut ExaWebSocket, u16, usize) -> Result<F, SqlxError>,
    F: Future<Output = Result<(DataChunk, &'a mut ExaWebSocket), SqlxError>> + 'a,
{
    pub fn new(
        ws: &'a mut ExaWebSocket,
        query_result: QueryResult,
        future_maker: C,
    ) -> Result<Self, SqlxError> {
        let stream = match query_result {
            QueryResult::ResultSet { result_set: rs } => Self::result_set(rs, ws, future_maker)?,
            QueryResult::RowCount { row_count } => Self::row_count(row_count),
        };

        Ok(stream)
    }

    fn row_count(row_count: u64) -> Self {
        let output = ExaQueryResult::new(row_count);
        let stream = stream::once(std::future::ready(Ok(output)));
        Self::RowCount(stream)
    }

    fn result_set(
        rs: ResultSet,
        ws: &'a mut ExaWebSocket,
        future_maker: C,
    ) -> Result<Self, SqlxError> {
        let stream = ResultSetStream::new(rs, ws, future_maker)?;
        Ok(Self::ResultSet(stream))
    }
}

impl<'a, C, F> Stream for QueryResultStream<'a, C, F>
where
    C: Fn(&'a mut ExaWebSocket, u16, usize) -> Result<F, SqlxError>,
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
    C: Fn(&'a mut ExaWebSocket, u16, usize) -> Result<F, SqlxError>,
    F: Future<Output = Result<(DataChunk, &'a mut ExaWebSocket), SqlxError>> + 'a,
{
    #[pin]
    chunk_stream: ChunkStream<'a, C, F>,
    chunk_iter: ChunkIter,
}

impl<'a, C, F> ResultSetStream<'a, C, F>
where
    C: Fn(&'a mut ExaWebSocket, u16, usize) -> Result<F, SqlxError>,
    F: Future<Output = Result<(DataChunk, &'a mut ExaWebSocket), SqlxError>> + 'a,
{
    fn new(rs: ResultSet, ws: &'a mut ExaWebSocket, future_maker: C) -> Result<Self, SqlxError> {
        let ResultSet {
            total_rows_num,
            total_rows_pos,
            output,
            columns,
        } = rs;

        let chunk_stream = match output {
            ResultSetOutput::Handle(handle) => {
                let future = future_maker(ws, handle, total_rows_pos)?;
                let future = Some(future);

                let chunk_stream = MultiChunkStream {
                    handle,
                    future_maker,
                    future,
                    total_rows_num,
                    total_rows_pos,
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

impl<'a, C, F> Stream for ResultSetStream<'a, C, F>
where
    C: Fn(&'a mut ExaWebSocket, u16, usize) -> Result<F, SqlxError>,
    F: Future<Output = Result<(DataChunk, &'a mut ExaWebSocket), SqlxError>> + 'a,
{
    type Item = Result<ExaRow, SqlxError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().project();

        if let Some(row) = this.chunk_iter.next() {
            return Poll::Ready(Some(Ok(row)));
        }

        let chunk = match ready!(this.chunk_stream.poll_next(cx)?) {
            Some(chunk) => chunk,
            None => return Poll::Ready(None),
        };

        this.chunk_iter.renew(chunk);
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

/// Enum keeping track of the data chunks we expect from the server.
/// If there are less than 1000 rows in the result set, Exasol directly sends them.
/// Hence, we have a single chunk stream variant.
///
/// If there are more, we need to fetch chunks one by one using a [`MultiChunkStream`].
#[pin_project(project = ChunkStreamProj)]
enum ChunkStream<'a, C, F>
where
    C: Fn(&'a mut ExaWebSocket, u16, usize) -> Result<F, SqlxError>,
    F: Future<Output = Result<(DataChunk, &'a mut ExaWebSocket), SqlxError>> + 'a,
{
    Multi(#[pin] MultiChunkStream<'a, C, F>),
    Single(#[pin] Once<Ready<Result<DataChunk, SqlxError>>>),
}

impl<'a, C, F> Stream for ChunkStream<'a, C, F>
where
    C: Fn(&'a mut ExaWebSocket, u16, usize) -> Result<F, SqlxError>,
    F: Future<Output = Result<(DataChunk, &'a mut ExaWebSocket), SqlxError>> + 'a,
{
    type Item = Result<DataChunk, SqlxError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.project() {
            ChunkStreamProj::Multi(s) => s.poll_next(cx),
            ChunkStreamProj::Single(s) => s.poll_next(cx),
        }
    }
}

/// A stream of chunks for a given result set that was given a handle.
#[pin_project]
struct MultiChunkStream<'a, C, F>
where
    C: Fn(&'a mut ExaWebSocket, u16, usize) -> Result<F, SqlxError>,
    F: Future<Output = Result<(DataChunk, &'a mut ExaWebSocket), SqlxError>> + 'a,
{
    #[pin]
    future: Option<F>,
    future_maker: C,
    handle: u16,
    total_rows_num: usize,
    total_rows_pos: usize,
}

impl<'a, C, F> MultiChunkStream<'a, C, F>
where
    C: Fn(&'a mut ExaWebSocket, u16, usize) -> Result<F, SqlxError>,
    F: Future<Output = Result<(DataChunk, &'a mut ExaWebSocket), SqlxError>> + 'a,
{
    fn update_future(
        self: Pin<&mut Self>,
        ws: &'a mut ExaWebSocket,
    ) -> Result<Option<F>, SqlxError> {
        if self.total_rows_pos >= self.total_rows_num {
            return Ok(None);
        }

        let this = self.project();
        let future = (this.future_maker)(ws, *this.handle, *this.total_rows_pos)?;
        Ok(Some(future))
    }
}

impl<'a, C, F> Stream for MultiChunkStream<'a, C, F>
where
    C: Fn(&'a mut ExaWebSocket, u16, usize) -> Result<F, SqlxError>,
    F: Future<Output = Result<(DataChunk, &'a mut ExaWebSocket), SqlxError>> + 'a,
{
    type Item = Result<DataChunk, SqlxError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().project();

        let (chunk, ws) = match this.future.as_pin_mut() {
            Some(f) => ready!(f.poll(cx)?),
            None => return Poll::Ready(None),
        };

        let future = self.as_mut().update_future(ws)?;

        let mut this = self.project();
        *this.total_rows_pos += chunk.num_rows;
        this.future.set(future);

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
            data: Arc::new([]),
        }
    }
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
