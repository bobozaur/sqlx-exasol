use std::{
    fmt::Debug,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{ready, Context, Poll},
};

use async_tungstenite::tungstenite::protocol::CloseFrame;
use futures_util::{SinkExt, StreamExt};
use serde::{
    de::{DeserializeOwned, IgnoredAny},
    Serialize,
};
use sqlx_core::Error as SqlxError;

use crate::{
    connection::{
        query_splitter::split_queries, stream::MultiResultStream, websocket::ExaWebSocket,
    },
    error::ExaProtocolError,
    request::{
        self, ClosePreparedStmt, CreatePreparedStmt, ExaLoginRequest, ExecutePreparedStmt, Fetch,
        LoginCreds, LoginRef, LoginToken, WithAttributes,
    },
    responses::{
        DataChunk, DescribeStatement, ExaResult, MultiResults, PreparedStatement, PublicKey,
        SingleResult,
    },
    ExaArguments, SessionInfo,
};

/// Adapter that wraps a type implementing [`WebSocketFuture`] to
/// allow interacting with it through a regular [`Future`].
///
/// Gets constructed with [`WebSocketFuture::future`].
#[derive(Debug)]
pub struct ExaFuture<'a, T> {
    inner: T,
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

    fn future(self, ws: &mut ExaWebSocket) -> ExaFuture<'_, Self> {
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
                    let command = ExecutePreparedStmt::new(
                        prepared.statement_handle,
                        prepared.parameters.clone(),
                        buf,
                    );

                    let future = ExaRoundtrip::new(command);
                    self.state = ExecutePreparedState::ExecutePrepared(future);
                }
                ExecutePreparedState::ExecutePrepared(future) => {
                    return future.poll_unpin(cx, ws).map_ok(From::from);
                }
            }
        }
    }
}

enum ExecutePreparedState<'a> {
    GetOrPrepare(GetOrPrepare<'a>),
    ExecutePrepared(ExaRoundtrip<ExecutePreparedStmt, SingleResult>),
}

pub struct ExecuteBatch<'a>(ExaRoundtrip<request::ExecuteBatch<'a>, MultiResults>);

impl<'a> ExecuteBatch<'a> {
    pub fn new(sql: &'a str) -> Self {
        let request = request::ExecuteBatch(split_queries(sql));
        Self(ExaRoundtrip::new(request))
    }
}

impl<'a> WebSocketFuture for ExecuteBatch<'a> {
    type Output = MultiResultStream;

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<Result<Self::Output, SqlxError>> {
        let multi_results = ready!(self.0.poll_unpin(cx, ws))?;
        Poll::Ready(multi_results.try_into())
    }
}

pub struct Execute<'a>(ExaRoundtrip<request::Execute<'a>, SingleResult>);

impl<'a> Execute<'a> {
    pub fn new(sql: &'a str) -> Self {
        Self(ExaRoundtrip::new(request::Execute(sql.into())))
    }
}

impl<'a> WebSocketFuture for Execute<'a> {
    type Output = MultiResultStream;

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<Result<Self::Output, SqlxError>> {
        self.0.poll_unpin(cx, ws).map_ok(From::from)
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

#[derive(Debug)]
pub struct FetchChunk(ExaRoundtrip<Fetch, DataChunk>);

impl FetchChunk {
    pub fn new(handle: u16, pos: usize, num_bytes: usize) -> Self {
        Self(ExaRoundtrip::new(Fetch::new(handle, pos, num_bytes)))
    }
}

impl WebSocketFuture for FetchChunk {
    type Output = DataChunk;

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<Result<Self::Output, SqlxError>> {
        self.0.poll_unpin(cx, ws)
    }
}

#[derive(Debug)]
pub enum Describe<'a> {
    CreatePrepared(ExaRoundtrip<CreatePreparedStmt<'a>, DescribeStatement>),
    ClosePrepared(ClosePrepared, DescribeStatement),
    Finished,
}

impl<'a> Describe<'a> {
    pub fn new(sql: &'a str) -> Self {
        Self::CreatePrepared(ExaRoundtrip::new(CreatePreparedStmt(sql)))
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
            match self {
                Self::CreatePrepared(future) => {
                    let describe = ready!(future.poll_unpin(cx, ws))?;
                    let future = ClosePrepared::new(describe.statement_handle);
                    *self = Self::ClosePrepared(future, describe);
                }
                Self::ClosePrepared(future, describe) => {
                    ready!(future.poll_unpin(cx, ws))?;
                    let describe = std::mem::take(describe);
                    *self = Self::Finished;
                    return Poll::Ready(Ok(describe));
                }
                Self::Finished => return Poll::Pending,
            }
        }
    }
}

#[derive(Debug)]
pub struct CreatePrepared<'a>(ExaRoundtrip<CreatePreparedStmt<'a>, PreparedStatement>);

impl<'a> CreatePrepared<'a> {
    pub fn new(sql: &'a str) -> Self {
        Self(ExaRoundtrip::new(CreatePreparedStmt(sql)))
    }
}

impl<'a> WebSocketFuture for CreatePrepared<'a> {
    type Output = PreparedStatement;

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<Result<Self::Output, SqlxError>> {
        self.0.poll_unpin(cx, ws)
    }
}

#[derive(Debug)]
pub struct ClosePrepared(ExaRoundtrip<ClosePreparedStmt, Option<IgnoredAny>>);

impl ClosePrepared {
    pub fn new(handle: u16) -> Self {
        Self(ExaRoundtrip::new(ClosePreparedStmt(handle)))
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

#[derive(Debug)]
pub struct CloseResultSets(ExaRoundtrip<request::CloseResultSets, Option<IgnoredAny>>);

impl CloseResultSets {
    pub fn new(handles: Vec<u16>) -> Self {
        Self(ExaRoundtrip::new(request::CloseResultSets(handles)))
    }
}

impl WebSocketFuture for CloseResultSets {
    type Output = ();

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<Result<Self::Output, SqlxError>> {
        self.0.poll_unpin(cx, ws).map_ok(|_| ())
    }
}

#[cfg(feature = "etl")]
#[derive(Debug)]
pub struct GetHosts(ExaRoundtrip<request::GetHosts, crate::responses::Hosts>);

#[cfg(feature = "etl")]
impl GetHosts {
    pub fn new(host_ip: std::net::IpAddr) -> Self {
        Self(ExaRoundtrip::new(request::GetHosts(host_ip)))
    }
}

#[cfg(feature = "etl")]
impl WebSocketFuture for GetHosts {
    type Output = crate::responses::Hosts;

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<Result<Self::Output, SqlxError>> {
        self.0.poll_unpin(cx, ws)
    }
}

#[derive(Debug, Default)]
pub struct SetAttributes(ExaRoundtrip<request::SetAttributes, Option<IgnoredAny>>);

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

#[derive(Debug, Default)]
pub struct GetAttributes(ExaRoundtrip<request::GetAttributes, Option<IgnoredAny>>);

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

#[derive(Debug)]
pub struct Commit(ExaRoundtrip<request::Execute<'static>, Option<IgnoredAny>>);

impl Default for Commit {
    fn default() -> Self {
        Self(ExaRoundtrip::new(request::Execute("COMMIT;".into())))
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

        // We explicitly set the attribute after we know the commit was sent.
        // Receiving the response is not needed for the commit to take place
        // on the database side.
        if self.0.has_sent() {
            ws.attributes.set_autocommit(true);
        }

        res
    }
}

#[derive(Debug)]
pub struct Rollback(ExaRoundtrip<request::Execute<'static>, Option<IgnoredAny>>);

impl Default for Rollback {
    fn default() -> Self {
        Self(ExaRoundtrip::new(request::Execute("ROLLBACK;".into())))
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

        // We explicitly set the attribute after we know the rollback was sent.
        // Receiving the response is not needed for the rollback to take place
        // on the database side.
        if self.0.has_sent() {
            ws.attributes.set_autocommit(true);
        }

        res
    }
}

#[derive(Default)]
pub struct Disconnect(ExaRoundtrip<request::Disconnect, Option<IgnoredAny>>);

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

/// The login process consists of sending the desired login command,
/// optionally receiving some response data from it, and then
/// finishing the login by sending the connection options.
///
/// It is ALWAYS uncompressed.
pub struct ExaLogin<'a> {
    opts: Option<ExaLoginRequest<'a>>,
    state: LoginState<'a>,
}

impl<'a> ExaLogin<'a> {
    pub fn new(opts: ExaLoginRequest<'a>) -> Self {
        let state = match opts.login {
            LoginRef::Credentials { .. } => {
                let command = LoginCreds(opts.protocol_version);
                LoginState::Credentials(ExaRoundtrip::new(command))
            }
            LoginRef::AccessToken { .. } | LoginRef::RefreshToken { .. } => {
                let command = LoginToken(opts.protocol_version);
                LoginState::Token(ExaRoundtrip::new(command))
            }
        };

        Self {
            opts: Some(opts),
            state,
        }
    }
}

impl WebSocketFuture for ExaLogin<'_> {
    type Output = SessionInfo;

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<Result<Self::Output, SqlxError>> {
        loop {
            match &mut self.state {
                LoginState::Token(future) => {
                    ready!(future.poll_unpin(cx, ws))?;
                    let command = self.opts.take().unwrap();
                    self.state = LoginState::Complete(ExaRoundtrip::new(command));
                }
                LoginState::Credentials(future) => {
                    let public_key = ready!(future.poll_unpin(cx, ws))?.into();
                    let mut command = self.opts.take().unwrap();
                    command.encrypt_password(&public_key)?;
                    self.state = LoginState::Complete(ExaRoundtrip::new(command));
                }
                LoginState::Complete(future) => return future.poll_unpin(cx, ws),
            }
        }
    }
}

enum LoginState<'a> {
    Token(ExaRoundtrip<LoginToken, Option<IgnoredAny>>),
    Credentials(ExaRoundtrip<LoginCreds, PublicKey>),
    Complete(ExaRoundtrip<ExaLoginRequest<'a>, SessionInfo>),
}

#[derive(Debug)]
pub enum ExaRoundtrip<REQ, OUT> {
    Waiting(REQ),
    Flushing,
    Receiving(PhantomData<fn() -> OUT>),
    Finished,
}

impl<REQ, OUT> ExaRoundtrip<REQ, OUT> {
    pub fn new(request: REQ) -> Self {
        Self::Waiting(request)
    }

    fn has_sent(&self) -> bool {
        match self {
            Self::Waiting(_) | Self::Flushing => false,
            Self::Receiving(_) | Self::Finished => true,
        }
    }
}

impl<REQ, OUT> Default for ExaRoundtrip<REQ, OUT>
where
    REQ: Default,
{
    fn default() -> Self {
        Self::new(REQ::default())
    }
}

impl<REQ, OUT> WebSocketFuture for ExaRoundtrip<REQ, OUT>
where
    REQ: Unpin,
    for<'a> WithAttributes<'a, REQ>: Serialize,
    OUT: Debug,
    ExaResult<OUT>: DeserializeOwned,
{
    type Output = OUT;

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<Result<Self::Output, SqlxError>> {
        loop {
            match self {
                Self::Waiting(request) => {
                    // Check if we need to rollback a transaction.
                    // We need to take the future out of the websocket for ownership reasons.
                    if let Some(mut future) = ws.pending_rollback.take() {
                        // If the rollback is not over yet, we register the future back.
                        if future.poll_unpin(cx, ws)?.is_pending() {
                            ws.pending_rollback = Some(future);
                            return Poll::Pending;
                        }
                    }

                    // Check if we need to close some open result sets.
                    // We need to take the future out of the websocket for ownership reasons.
                    if let Some(mut future) = ws.pending_close.take() {
                        // If closing the result sets is not over yet, we register the future back.
                        if future.poll_unpin(cx, ws)?.is_pending() {
                            ws.pending_close = Some(future);
                            return Poll::Pending;
                        }
                    }

                    if ws.active_request {
                        ready!(ws.poll_flush_unpin(cx))?;
                        ready!(ws.poll_next_unpin(cx)).transpose()?;
                        ws.active_request = false;
                    }

                    ready!(ws.poll_ready_unpin(cx))?;
                    let with_attrs = WithAttributes::new(request, &ws.attributes);
                    let request = serde_json::to_string(&with_attrs)
                        .map_err(|e| SqlxError::Protocol(e.to_string()))?;

                    tracing::trace!("sending request:\n{request}");
                    ws.start_send_unpin(request)?;
                    ws.attributes.set_needs_send(false);
                    ws.active_request = true;

                    *self = Self::Flushing;
                }
                Self::Flushing => {
                    ready!(ws.poll_flush_unpin(cx))?;
                    *self = Self::Receiving(PhantomData);
                }
                Self::Receiving(_) => {
                    let Some(bytes) = ready!(ws.poll_next_unpin(cx)).transpose()? else {
                        return Err(ExaProtocolError::from(None::<CloseFrame>))?;
                    };

                    ws.active_request = false;

                    let (out, attr_opt) =
                        match serde_json::from_slice(&bytes).map_err(ExaProtocolError::from)? {
                            ExaResult::Ok {
                                response_data,
                                attributes,
                            } => (response_data, attributes),
                            ExaResult::Error { exception } => Err(exception)?,
                        };

                    if let Some(attributes) = attr_opt {
                        tracing::debug!("updating connection attributes using:\n{attributes:#?}");
                        ws.attributes.update(attributes);
                    }

                    tracing::trace!("database response:\n{out:#?}");
                    *self = Self::Finished;
                    return Poll::Ready(Ok(out));
                }
                Self::Finished => return Poll::Pending,
            }
        }
    }
}
