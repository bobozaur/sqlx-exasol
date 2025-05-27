use std::{
    fmt::Debug,
    future::Future,
    marker::PhantomData,
    net::IpAddr,
    pin::Pin,
    task::{ready, Context, Poll},
};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use lru::LruCache;
use serde::de::IgnoredAny;
use sqlx_core::Error as SqlxError;

use crate::{
    arguments::ExaBuffer,
    command::ExaCommand,
    connection::websocket::ExaWebSocket,
    error::ExaProtocolError,
    responses::{
        DescribeStatement, ExaResult, Hosts, MultiResults, PreparedStatement, QueryResult,
        SingleResult,
    },
    ExaAttributes,
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
        this.inner.poll_unpin(cx, &mut this.ws)
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
    sql: &'a str,
    attributes: &'a ExaAttributes,
    cache: &'a mut LruCache<String, PreparedStatement>,
    persist: bool,
    buf: ExaBuffer,
    state: ExecutePreparedState,
}

impl<'a> ExecutePrepared<'a> {
    pub fn new(
        sql: &'a str,
        attributes: &'a ExaAttributes,
        cache: &'a mut LruCache<String, PreparedStatement>,
        persist: bool,
        buf: ExaBuffer,
    ) -> Self {
        let state = ExecutePreparedState::GetOrPrepare;

        Self {
            sql,
            attributes,
            cache,
            persist,
            buf,
            state,
        }
    }
}

impl<'a> WebSocketFuture for ExecutePrepared<'a> {
    type Output = QueryResult;

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<Result<Self::Output, SqlxError>> {
        loop {
            match &mut self.state {
                ExecutePreparedState::GetOrPrepare => {
                    self.state = if self.cache.contains(self.sql) {
                        ExecutePreparedState::GetCached
                    } else {
                        let future = CreatePrepared::new(self.sql)?;
                        ExecutePreparedState::CreatePrepared(future)
                    };
                }
                ExecutePreparedState::CreatePrepared(future) => {
                    let prepared = ready!(future.poll_unpin(cx, ws))?;

                    self.state = if self.persist {
                        if let Some((_, old)) = self.cache.push(self.sql.to_owned(), prepared) {
                            let future = ClosePrepared::new(old.statement_handle)?;
                            ExecutePreparedState::ClosePrepared(future)
                        } else {
                            ExecutePreparedState::GetCached
                        }
                    } else {
                        ExecutePreparedState::NonPersistent(prepared)
                    }
                }
                ExecutePreparedState::ClosePrepared(future) => {
                    ready!(future.poll_unpin(cx, ws))?;
                    self.state = ExecutePreparedState::GetCached;
                }
                ExecutePreparedState::GetCached => {
                    let prepared = self
                        .cache
                        .get(self.sql)
                        .expect("prepared statement must be in cache");

                    let buf = std::mem::take(&mut self.buf);

                    let command = ExaCommand::new_execute_prepared(
                        prepared.statement_handle,
                        &prepared.parameters,
                        buf,
                        self.attributes,
                    );

                    let future = command.try_into().and_then(GetQueryResults::new)?;
                    self.state = ExecutePreparedState::ExecutePrepared(future);
                }
                ExecutePreparedState::NonPersistent(prepared) => {
                    let buf = std::mem::take(&mut self.buf);

                    let command = ExaCommand::new_execute_prepared(
                        prepared.statement_handle,
                        &prepared.parameters,
                        buf,
                        self.attributes,
                    );

                    let future = command.try_into().and_then(GetQueryResults::new)?;
                    self.state = ExecutePreparedState::ExecutePrepared(future);
                }
                ExecutePreparedState::ExecutePrepared(future) => {
                    return future.poll_unpin(cx, ws);
                }
            }
        }
    }
}

enum ExecutePreparedState {
    GetOrPrepare,
    CreatePrepared(CreatePrepared),
    ClosePrepared(ClosePrepared),
    GetCached,
    NonPersistent(PreparedStatement),
    ExecutePrepared(GetQueryResults<SingleResult, QueryResult>),
}

pub struct ExecuteBatch(GetQueryResults<MultiResults, Vec<QueryResult>>);

impl ExecuteBatch {
    pub fn new(sql_texts: &[&str], attributes: &ExaAttributes) -> Result<Self, SqlxError> {
        let command = ExaCommand::new_execute_batch(sql_texts, attributes).try_into()?;
        GetQueryResults::new(command).map(Self)
    }
}

impl WebSocketFuture for ExecuteBatch {
    type Output = Vec<QueryResult>;

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<Result<Self::Output, SqlxError>> {
        self.0.poll_unpin(cx, ws)
    }
}

pub struct Execute(GetQueryResults<SingleResult, QueryResult>);

impl Execute {
    pub fn new(sql: &str, attributes: &ExaAttributes) -> Result<Self, SqlxError> {
        let command = ExaCommand::new_execute(sql, attributes).try_into()?;
        GetQueryResults::new(command).map(Self)
    }
}

impl WebSocketFuture for Execute {
    type Output = QueryResult;

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<Result<Self::Output, SqlxError>> {
        self.0.poll_unpin(cx, ws)
    }
}

struct GetQueryResults<T, U> {
    state: GetQueryResultsState<T>,
    command: String,
    _marker: PhantomData<fn() -> U>,
}

impl<T, U> GetQueryResults<T, U> {
    fn new(command: String) -> Result<Self, SqlxError> {
        let _marker = PhantomData;
        let state = GetQueryResultsState::CheckLastResultSet;
        Ok(Self {
            state,
            command,
            _marker,
        })
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
                            GetQueryResultsState::CloseResultSet(CloseResultSet::new(handle)?)
                        }
                        None => GetQueryResultsState::GetQueryResult(ExaTransport::from_command(
                            std::mem::take(&mut self.command),
                        )),
                    };
                }
                GetQueryResultsState::CloseResultSet(future) => {
                    let res = future.poll_unpin(cx, ws);

                    // Unregister the last result set handle once we know it got closed.
                    if future.0.has_sent() {
                        ws.last_rs_handle = None;
                    }

                    ready!(res)?;
                    let transport = ExaTransport::from_command(std::mem::take(&mut self.command));
                    self.state = GetQueryResultsState::GetQueryResult(transport);
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
    GetQueryResult(ExaTransport<T>),
}

pub struct CloseResultSet(ExaTransport<Option<IgnoredAny>>);

impl CloseResultSet {
    pub fn new(handle: u16) -> Result<Self, SqlxError> {
        let command = ExaCommand::new_close_result(handle).try_into()?;
        Ok(Self(ExaTransport::from_command(command)))
    }
}

impl WebSocketFuture for CloseResultSet {
    type Output = ();

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<Result<Self::Output, SqlxError>> {
        self.0.poll_unpin(cx, ws).map_ok(|_| ())
    }
}

pub struct CreatePrepared(ExaTransport<PreparedStatement>);

impl CreatePrepared {
    pub fn new(sql: &str) -> Result<Self, SqlxError> {
        let command = ExaCommand::new_create_prepared(sql).try_into()?;
        Ok(Self(ExaTransport::from_command(command)))
    }
}

impl WebSocketFuture for CreatePrepared {
    type Output = PreparedStatement;

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<Result<Self::Output, SqlxError>> {
        self.0.poll_unpin(cx, ws)
    }
}

pub struct ClosePrepared(ExaTransport<Option<IgnoredAny>>);

impl ClosePrepared {
    pub fn new(handle: u16) -> Result<Self, SqlxError> {
        let command = ExaCommand::new_close_prepared(handle).try_into()?;
        Ok(Self(ExaTransport::from_command(command)))
    }
}

impl WebSocketFuture for ClosePrepared {
    type Output = ();

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<Result<Self::Output, SqlxError>> {
        self.0.poll_unpin(cx, ws).map_ok(|_| ())
    }
}

pub struct Describe(ExaTransport<DescribeStatement>);

impl Describe {
    pub fn new(sql: &str) -> Result<Self, SqlxError> {
        let command = ExaCommand::new_create_prepared(sql).try_into()?;
        Ok(Self(ExaTransport::from_command(command)))
    }
}

impl WebSocketFuture for Describe {
    type Output = DescribeStatement;

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<Result<Self::Output, SqlxError>> {
        self.0.poll_unpin(cx, ws)
    }
}

#[cfg(feature = "etl")]
pub struct GetHosts(ExaTransport<Hosts>);

#[cfg(feature = "etl")]
impl GetHosts {
    pub fn new(host_ip: IpAddr) -> Result<Self, SqlxError> {
        let command = ExaCommand::new_get_hosts(host_ip).try_into()?;
        Ok(Self(ExaTransport::from_command(command)))
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
        self.0.poll_unpin(cx, ws)
    }
}

pub struct SetAttributes(ExaTransport<Option<IgnoredAny>>);

impl SetAttributes {
    pub fn new(attributes: &ExaAttributes) -> Result<Self, SqlxError> {
        let command = ExaCommand::new_set_attributes(attributes).try_into()?;
        Ok(Self(ExaTransport::from_command(command)))
    }
}

impl WebSocketFuture for SetAttributes {
    type Output = ();

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<Result<Self::Output, SqlxError>> {
        self.0.poll_unpin(cx, ws).map_ok(|_| ())
    }
}

pub struct GetAttributes(ExaTransport<Option<IgnoredAny>>);

impl GetAttributes {
    pub fn new() -> Result<Self, SqlxError> {
        let command = ExaCommand::GetAttributes.try_into()?;
        Ok(Self(ExaTransport::from_command(command)))
    }
}

impl WebSocketFuture for GetAttributes {
    type Output = ();

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<Result<Self::Output, SqlxError>> {
        self.0.poll_unpin(cx, ws).map_ok(|_| ())
    }
}

pub struct Commit(ExaTransport<Option<IgnoredAny>>);

impl Commit {
    pub fn new(attributes: &ExaAttributes) -> Result<Self, SqlxError> {
        let command = ExaCommand::new_execute("COMMIT;", attributes).try_into()?;
        Ok(Self(ExaTransport::from_command(command)))
    }
}

impl WebSocketFuture for Commit {
    type Output = ();

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<Result<Self::Output, SqlxError>> {
        let res = self.0.poll_unpin(cx, ws).map_ok(|_| ());

        if self.0.has_sent() {
            // We explicitly set the attribute after we know the rollback was sent.
            // Receiving the response is not needed for the rollback to take place
            // on the database side.
            ws.attributes.set_autocommit(true);
            ws.attributes.set_open_transaction(false);
        }

        res
    }
}

pub struct Rollback(ExaTransport<Option<IgnoredAny>>);

impl Rollback {
    pub fn new(attributes: &ExaAttributes) -> Result<Self, SqlxError> {
        let command = ExaCommand::new_execute("ROLLBACK;", attributes).try_into()?;
        Ok(Self(ExaTransport::from_command(command)))
    }
}

impl WebSocketFuture for Rollback {
    type Output = ();

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<Result<Self::Output, SqlxError>> {
        let res = self.0.poll_unpin(cx, ws).map_ok(|_| ());

        if self.0.has_sent() {
            // We explicitly set the attribute after we know the rollback was sent.
            // Receiving the response is not needed for the rollback to take place
            // on the database side.
            ws.attributes.set_autocommit(true);
            ws.attributes.set_open_transaction(false);
        }

        res
    }
}

pub struct Disconnect(ExaTransport<Option<IgnoredAny>>);

impl Disconnect {
    pub fn new() -> Result<Self, SqlxError> {
        let command = ExaCommand::Disconnect.try_into()?;
        Ok(Self(ExaTransport::from_command(command)))
    }
}

impl WebSocketFuture for Disconnect {
    type Output = ();

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<Result<Self::Output, SqlxError>> {
        self.0.poll_unpin(cx, ws).map_ok(|_| ())
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
        return Poll::Ready(Ok(out));
    }
}
