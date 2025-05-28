use std::{
    fmt::Debug,
    future::Future,
    marker::PhantomData,
    net::IpAddr,
    pin::Pin,
    task::{ready, Context, Poll},
};

use futures_util::{SinkExt, StreamExt};
use serde::de::IgnoredAny;
use sqlx_core::{bytes::Bytes, Error as SqlxError};

use crate::{
    command::ExaCommand,
    connection::{
        query_splitter::split_queries, stream::MultiResultStream, websocket::ExaWebSocket,
    },
    error::ExaProtocolError,
    responses::{
        DataChunk, DescribeStatement, ExaResult, Hosts, MultiResults, PreparedStatement,
        QueryResult, SingleResult,
    },
    ExaArguments,
};

/// Adapter that wraps a type implementing [`WebSocketFuture`] to
/// allow interacting with it through a regular [`Future`].
///
/// Gets constructed with [`WebSocketFuture::future`].
pub struct ExaFuture<'a, T> {
    inner: &'a mut T,
    ws: &'a mut ExaWebSocket,
}

impl<'a, T> Future for ExaFuture<'a, T>
where
    T: WebSocketFuture,
{
    type Output = Result<T::Output, SqlxError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.inner.poll_unpin(cx, this.ws)
    }
}

pub trait WebSocketFuture: Unpin + Sized {
    type Output;

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<Result<Self::Output, SqlxError>>;

    fn future<'a>(&'a mut self, ws: &'a mut ExaWebSocket) -> ExaFuture<'a, Self> {
        ExaFuture { inner: self, ws }
    }
}

pub struct ExecutePrepared<'a> {
    arguments: ExaArguments,
    state: ExecutePreparedState<'a>,
}

impl<'a> ExecutePrepared<'a> {
    pub fn new(sql: &'a str, persist: bool, arguments: ExaArguments) -> Self {
        let future = GetOrPrepare::new(sql, persist);
        let state = ExecutePreparedState::GetOrPrepare(future);
        Self { arguments, state }
    }
}

impl<'a> WebSocketFuture for ExecutePrepared<'a> {
    type Output = MultiResultStream;

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<Result<Self::Output, SqlxError>> {
        loop {
            match &mut self.state {
                ExecutePreparedState::GetOrPrepare(future) => {
                    let prepared = ready!(future.poll_unpin(cx, ws))?;

                    // Check the compatibility between provided parameter data types
                    // and the ones expected by the database.
                    let iter = std::iter::zip(prepared.parameters.as_ref(), &self.arguments.types);
                    for (expected, provided) in iter {
                        if !expected.compatible(provided) {
                            return Err(ExaProtocolError::DatatypeMismatch(
                                expected.name,
                                provided.name,
                            ))?;
                        }
                    }

                    let buf = std::mem::take(&mut self.arguments.buf);
                    let command = ExaCommand::new_execute_prepared(
                        prepared.statement_handle,
                        &prepared.parameters,
                        buf,
                        &ws.attributes,
                    );

                    let future = GetQueryResults::new(command.try_into()?);
                    self.state = ExecutePreparedState::ExecutePrepared(future);
                }
                ExecutePreparedState::ExecutePrepared(future) => {
                    return future
                        .poll_unpin(cx, ws)
                        .map_ok(|qr| MultiResultStream::new(qr, Vec::new().into_iter()));
                }
            }
        }
    }
}

enum ExecutePreparedState<'a> {
    GetOrPrepare(GetOrPrepare<'a>),
    ExecutePrepared(GetQueryResults<SingleResult, QueryResult>),
}

/// TODO: Document the states here
pub struct ExecuteBatch<'a> {
    sql_texts: Vec<&'a str>,
    future: Option<GetQueryResults<MultiResults, Vec<QueryResult>>>,
}

impl<'a> ExecuteBatch<'a> {
    pub fn new(sql: &'a str) -> Self {
        Self {
            sql_texts: split_queries(sql),
            future: None,
        }
    }
}

impl<'a> WebSocketFuture for ExecuteBatch<'a> {
    type Output = MultiResultStream;

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<Result<Self::Output, SqlxError>> {
        loop {
            match &mut self.future {
                None => {
                    let command = ExaCommand::new_execute_batch(&self.sql_texts, &ws.attributes)
                        .try_into()?;
                    let future = GetQueryResults::new(command);
                    self.future = Some(future);
                }
                Some(future) => {
                    let mut results_iter = ready!(future.poll_unpin(cx, ws))?.into_iter();

                    let Some(first_result) = results_iter.next() else {
                        return Err(ExaProtocolError::NoResponse)?;
                    };

                    return Poll::Ready(Ok(MultiResultStream::new(first_result, results_iter)));
                }
            }
        }
    }
}

pub struct Execute<'a> {
    sql: &'a str,
    future: Option<GetQueryResults<SingleResult, QueryResult>>,
}

impl<'a> Execute<'a> {
    pub fn new(sql: &'a str) -> Self {
        Self { sql, future: None }
    }
}

impl<'a> WebSocketFuture for Execute<'a> {
    type Output = MultiResultStream;

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<Result<Self::Output, SqlxError>> {
        loop {
            match &mut self.future {
                None => {
                    let command = ExaCommand::new_execute(self.sql, &ws.attributes).try_into()?;
                    let future = GetQueryResults::new(command);
                    self.future = Some(future);
                }
                Some(future) => {
                    return future
                        .poll_unpin(cx, ws)
                        .map_ok(|qr| MultiResultStream::new(qr, Vec::new().into_iter()))
                }
            }
        }
    }
}

pub struct GetOrPrepare<'a> {
    sql: &'a str,
    persist: bool,
    state: GetOrPrepareState<'a>,
}

impl<'a> GetOrPrepare<'a> {
    pub fn new(sql: &'a str, persist: bool) -> Self {
        Self {
            sql,
            persist,
            state: GetOrPrepareState::GetCached,
        }
    }
}

impl<'a> WebSocketFuture for GetOrPrepare<'a> {
    type Output = PreparedStatement;

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<Result<Self::Output, SqlxError>> {
        loop {
            match &mut self.state {
                GetOrPrepareState::GetCached => {
                    self.state = match ws.statement_cache.get(self.sql).cloned() {
                        // Cache hit, simply return
                        Some(prepared) => return Poll::Ready(Ok(prepared)),
                        // Cache miss, switch state and prepare statement
                        None => GetOrPrepareState::CreatePrepared(CreatePrepared::new(self.sql)),
                    }
                }
                GetOrPrepareState::CreatePrepared(future) => {
                    let prepared = ready!(future.poll_unpin(cx, ws))?;

                    if !self.persist {
                        return Poll::Ready(Ok(prepared));
                    }

                    // Insert the prepared statement into the cache.
                    // This can evict an old entry, in which case we transition
                    // to closing it.
                    //
                    // Otherwise we go to simply retrieving it from the cache.
                    self.state = ws
                        .statement_cache
                        .push(self.sql.to_owned(), prepared)
                        .map(|(_, p)| p.statement_handle)
                        .map(ClosePrepared::new)
                        .map_or(
                            GetOrPrepareState::GetCached,
                            GetOrPrepareState::ClosePrepared,
                        );
                }
                GetOrPrepareState::ClosePrepared(future) => {
                    ready!(future.poll_unpin(cx, ws))?;
                    self.state = GetOrPrepareState::GetCached;
                }
            }
        }
    }
}

enum GetOrPrepareState<'a> {
    GetCached,
    CreatePrepared(CreatePrepared<'a>),
    ClosePrepared(ClosePrepared),
}

struct GetQueryResults<T, U> {
    state: GetQueryResultsState<T>,
    command: String,
    _marker: PhantomData<fn() -> U>,
}

impl<T, U> GetQueryResults<T, U> {
    fn new(command: String) -> Self {
        let state = GetQueryResultsState::CheckLastResultSet;

        Self {
            state,
            command,
            _marker: PhantomData,
        }
    }
}

impl<T, U> WebSocketFuture for GetQueryResults<T, U>
where
    U: From<T>,
    ExaTransport<T>: WebSocketFuture<Output = T>,
{
    type Output = U;

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<Result<Self::Output, SqlxError>> {
        loop {
            match &mut self.state {
                GetQueryResultsState::CheckLastResultSet => {
                    self.state = match ws.last_rs_handle {
                        Some(handle) => {
                            GetQueryResultsState::CloseResultSet(CloseResultSet::new(handle))
                        }
                        None => GetQueryResultsState::BuildFuture,
                    };
                }
                GetQueryResultsState::CloseResultSet(future) => {
                    let res = future.poll_unpin(cx, ws);

                    // Unregister the last result set handle once we know it got closed.
                    if future.has_sent() {
                        ws.last_rs_handle = None;
                    }

                    ready!(res)?;
                    self.state = GetQueryResultsState::BuildFuture;
                }
                GetQueryResultsState::BuildFuture => {
                    let command = std::mem::take(&mut self.command);
                    let future = ExaTransport::from_command(command);
                    self.state = GetQueryResultsState::GetQueryResult(future);
                }
                GetQueryResultsState::GetQueryResult(future) => {
                    return future.poll_unpin(cx, ws).map_ok(From::from);
                }
            }
        }
    }
}

enum GetQueryResultsState<T> {
    CheckLastResultSet,
    CloseResultSet(CloseResultSet),
    BuildFuture,
    GetQueryResult(ExaTransport<T>),
}

pub struct FetchChunk {
    handle: u16,
    pos: usize,
    num_bytes: usize,
    future: Option<ExaTransport<DataChunk>>,
}

impl FetchChunk {
    pub fn new(handle: u16, pos: usize, num_bytes: usize) -> Self {
        Self {
            handle,
            pos,
            num_bytes,
            future: None,
        }
    }
}

impl WebSocketFuture for FetchChunk {
    type Output = DataChunk;

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<Result<Self::Output, SqlxError>> {
        loop {
            if let Some(future) = self.future.as_mut() {
                return future.poll_unpin(cx, ws);
            }

            let command = ExaCommand::new_fetch(self.handle, self.pos, self.num_bytes);
            self.future = Some(ExaTransport::from_command(command.try_into()?));
        }
    }
}

pub struct CloseResultSet {
    handle: u16,
    future: Option<ExaTransport<Option<IgnoredAny>>>,
}

impl CloseResultSet {
    pub fn new(handle: u16) -> Self {
        Self {
            handle,
            future: None,
        }
    }

    fn has_sent(&self) -> bool {
        self.future.as_ref().is_some_and(ExaTransport::has_sent)
    }
}

impl WebSocketFuture for CloseResultSet {
    type Output = ();

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<Result<Self::Output, SqlxError>> {
        loop {
            if let Some(future) = self.future.as_mut() {
                return future.poll_unpin(cx, ws).map_ok(|_| ());
            }

            let command = ExaCommand::new_close_result(self.handle).try_into()?;
            self.future = Some(ExaTransport::from_command(command));
        }
    }
}

pub struct CreatePrepared<'a> {
    sql: &'a str,
    future: Option<ExaTransport<PreparedStatement>>,
}

impl<'a> CreatePrepared<'a> {
    pub fn new(sql: &'a str) -> Self {
        Self { sql, future: None }
    }
}

impl<'a> WebSocketFuture for CreatePrepared<'a> {
    type Output = PreparedStatement;

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<Result<Self::Output, SqlxError>> {
        loop {
            if let Some(future) = self.future.as_mut() {
                return future.poll_unpin(cx, ws);
            }

            let command = ExaCommand::new_create_prepared(self.sql).try_into()?;
            self.future = Some(ExaTransport::from_command(command));
        }
    }
}

pub struct ClosePrepared {
    handle: u16,
    future: Option<ExaTransport<Option<IgnoredAny>>>,
}

impl ClosePrepared {
    pub fn new(handle: u16) -> Self {
        Self {
            handle,
            future: None,
        }
    }
}

impl WebSocketFuture for ClosePrepared {
    type Output = ();

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<Result<Self::Output, SqlxError>> {
        loop {
            if let Some(future) = self.future.as_mut() {
                return future.poll_unpin(cx, ws).map_ok(|_| ());
            }

            let command = ExaCommand::new_close_prepared(self.handle).try_into()?;
            self.future = Some(ExaTransport::from_command(command));
        }
    }
}

pub struct Describe<'a> {
    sql: &'a str,
    future: Option<ExaTransport<DescribeStatement>>,
}

/// Use prepare & close prepared in the state machine
impl<'a> Describe<'a> {
    pub fn new(sql: &'a str) -> Self {
        Self { sql, future: None }
    }
}

impl<'a> WebSocketFuture for Describe<'a> {
    type Output = DescribeStatement;

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<Result<Self::Output, SqlxError>> {
        loop {
            if let Some(future) = self.future.as_mut() {
                return future.poll_unpin(cx, ws);
            }

            let command = ExaCommand::new_create_prepared(self.sql).try_into()?;
            self.future = Some(ExaTransport::from_command(command));
        }
    }
}

#[cfg(feature = "etl")]
pub struct GetHosts {
    host_ip: IpAddr,
    future: Option<ExaTransport<Hosts>>,
}

#[cfg(feature = "etl")]
impl GetHosts {
    pub fn new(host_ip: IpAddr) -> Self {
        Self {
            host_ip,
            future: None,
        }
    }
}

#[cfg(feature = "etl")]
impl WebSocketFuture for GetHosts {
    type Output = Hosts;

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<Result<Self::Output, SqlxError>> {
        loop {
            if let Some(future) = self.future.as_mut() {
                return future.poll_unpin(cx, ws);
            }

            let command = ExaCommand::new_get_hosts(self.host_ip).try_into()?;
            self.future = Some(ExaTransport::from_command(command));
        }
    }
}

#[derive(Default)]
pub struct SetAttributes(Option<ExaTransport<Option<IgnoredAny>>>);

impl WebSocketFuture for SetAttributes {
    type Output = ();

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<Result<Self::Output, SqlxError>> {
        loop {
            if let Some(future) = self.0.as_mut() {
                return future.poll_unpin(cx, ws).map_ok(|_| ());
            }

            let command = ExaCommand::new_set_attributes(&ws.attributes).try_into()?;
            self.0 = Some(ExaTransport::from_command(command));
        }
    }
}

#[derive(Default)]
pub struct GetAttributes(Option<ExaTransport<Option<IgnoredAny>>>);

impl WebSocketFuture for GetAttributes {
    type Output = ();

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<Result<Self::Output, SqlxError>> {
        loop {
            if let Some(future) = self.0.as_mut() {
                return future.poll_unpin(cx, ws).map_ok(|_| ());
            }

            let command = ExaCommand::GetAttributes.try_into()?;
            self.0 = Some(ExaTransport::from_command(command));
        }
    }
}

#[derive(Default)]
pub struct Commit(Option<ExaTransport<Option<IgnoredAny>>>);

impl WebSocketFuture for Commit {
    type Output = ();

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<Result<Self::Output, SqlxError>> {
        loop {
            if let Some(future) = self.0.as_mut() {
                let res = future.poll_unpin(cx, ws).map_ok(|_| ());

                // We explicitly set the attribute after we know the rollback was sent.
                // Receiving the response is not needed for the rollback to take place
                // on the database side.
                if future.has_sent() {
                    ws.attributes.set_autocommit(true);
                }

                return res;
            }

            let command = ExaCommand::new_execute("COMMIT;", &ws.attributes).try_into()?;
            self.0 = Some(ExaTransport::from_command(command));
        }
    }
}

#[derive(Default)]
pub struct Rollback(Option<ExaTransport<Option<IgnoredAny>>>);

impl WebSocketFuture for Rollback {
    type Output = ();

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<Result<Self::Output, SqlxError>> {
        loop {
            if let Some(future) = self.0.as_mut() {
                let res = future.poll_unpin(cx, ws).map_ok(|_| ());

                // We explicitly set the attribute after we know the rollback was sent.
                // Receiving the response is not needed for the rollback to take place
                // on the database side.
                if future.has_sent() {
                    ws.attributes.set_autocommit(true);
                }

                return res;
            }

            let command = ExaCommand::new_execute("ROLLBACK;", &ws.attributes).try_into()?;
            self.0 = Some(ExaTransport::from_command(command));
        }
    }
}

#[derive(Default)]
pub struct Disconnect(Option<ExaTransport<Option<IgnoredAny>>>);

impl WebSocketFuture for Disconnect {
    type Output = ();

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<Result<Self::Output, SqlxError>> {
        loop {
            if let Some(future) = self.0.as_mut() {
                return future.poll_unpin(cx, ws).map_ok(|_| ());
            }

            let command = ExaCommand::Disconnect.try_into()?;
            self.0 = Some(ExaTransport::from_command(command));
        }
    }
}

#[derive(Debug)]
enum ExaTransport<T> {
    Sending(ExaSend),
    Receiving(ExaRecv<T>),
    Finished,
}

impl<T> ExaTransport<T> {
    fn from_command(command: String) -> Self {
        Self::Sending(ExaSend::new(command))
    }

    fn has_sent(&self) -> bool {
        !matches!(self, ExaTransport::Sending(_))
    }
}

impl<T> WebSocketFuture for ExaTransport<T>
where
    T: Debug,
    ExaResult<T>: TryFrom<Bytes>,
    SqlxError: From<<ExaResult<T> as TryFrom<Bytes>>::Error>,
{
    type Output = T;

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<Result<Self::Output, SqlxError>> {
        loop {
            match self {
                ExaTransport::Sending(f) => {
                    ready!(f.poll_unpin(cx, ws))?;
                    *self = Self::Receiving(ExaRecv::default());
                }

                ExaTransport::Receiving(f) => {
                    let out = ready!(f.poll_unpin(cx, ws))?;
                    *self = ExaTransport::Finished;
                    return Poll::Ready(Ok(out));
                }
                ExaTransport::Finished => return Poll::Pending,
            }
        }
    }
}

#[derive(Debug)]
pub struct ExaSend(String);

impl ExaSend {
    pub fn new(command: String) -> Self {
        Self(command)
    }
}

impl WebSocketFuture for ExaSend {
    type Output = ();

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<Result<Self::Output, SqlxError>> {
        ready!(ws.poll_ready_unpin(cx))?;
        let command = std::mem::take(&mut self.0);
        tracing::trace!("sending command:\n{command}");
        ws.start_send_unpin(command)?;
        Poll::Ready(Ok(()))
    }
}

#[derive(Debug)]
pub struct ExaRecv<T>(PhantomData<fn() -> T>);

impl<T> Default for ExaRecv<T> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<T> WebSocketFuture for ExaRecv<T>
where
    T: Debug,
    ExaResult<T>: TryFrom<Bytes>,
    SqlxError: From<<ExaResult<T> as TryFrom<Bytes>>::Error>,
{
    type Output = T;

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<Result<Self::Output, SqlxError>> {
        let Some(bytes_res) = ready!(ws.poll_next_unpin(cx)) else {
            return Err(ExaProtocolError::from(None))?;
        };

        let (out, attr_opt) = Result::from(ExaResult::try_from(bytes_res?)?)?;

        if let Some(attributes) = attr_opt {
            tracing::debug!("updating connection attributes using:\n{attributes:#?}");
            ws.attributes.update(attributes);
        }

        tracing::trace!("database response:\n{out:#?}");
        Poll::Ready(Ok(out))
    }
}
