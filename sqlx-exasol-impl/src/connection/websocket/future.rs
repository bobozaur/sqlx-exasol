//! Module providing futures for various database interactions.
//!
//! The design had two main objectives:
//! - infallible future creation, with the request serialization happenning internally to better
//!   accommodate the [`sqlx_core::executor::Executor`] trait.
//! - allow composing multiple futures seamlessly, achieved through the [`WebSocketFuture`] trait.

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
use sqlx_core::type_info::TypeInfo;

use crate::{
    connection::{
        stream::MultiResultStream,
        websocket::{
            request::{
                self, ClosePreparedStmt, CreatePreparedStmt, ExaLoginRequest, ExecutePreparedStmt,
                Fetch, LoginCreds, LoginRef, LoginToken, WithAttributes,
            },
            ExaWebSocket,
        },
    },
    error::ExaProtocolError,
    responses::{
        DataChunk, DescribeStatement, ExaResult, MultiResults, PreparedStatement, PublicKey,
        SingleResult,
    },
    ExaArguments, SessionInfo, SqlxError, SqlxResult,
};

/// Adapter that wraps a type implementing [`WebSocketFuture`] to allow interacting with it through
/// a regular [`Future`].
///
/// Gets constructed with [`WebSocketFuture::future`].
#[derive(Debug)]
pub struct ExaFuture<'a, T> {
    inner: T,
    ws: &'a mut ExaWebSocket,
}

impl<T> Future for ExaFuture<'_, T>
where
    T: WebSocketFuture,
{
    type Output = SqlxResult<T::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.inner.poll_unpin(cx, this.ws)
    }
}

/// Helper trait for defining a future like interface which also accepts a [`ExaWebSocket`]
/// argument. This allows nesting types as needed and simply propagating the websocket as an
/// argument wherever polling is necessary.
pub trait WebSocketFuture: Unpin + Sized {
    type Output;

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<SqlxResult<Self::Output>>;

    fn future(self, ws: &mut ExaWebSocket) -> ExaFuture<'_, Self> {
        ExaFuture { inner: self, ws }
    }
}

/// Implementor of [`WebSocketFuture`] that executes a prepared statement.
#[derive(Debug)]
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

impl WebSocketFuture for ExecutePrepared<'_> {
    type Output = MultiResultStream;

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<SqlxResult<Self::Output>> {
        loop {
            match &mut self.state {
                ExecutePreparedState::GetOrPrepare(future) => {
                    let prepared = ready!(future.poll_unpin(cx, ws))?;

                    // Check the compatibility between provided parameter data types
                    // and the ones expected by the database.
                    let iter = std::iter::zip(prepared.parameters.as_ref(), &self.arguments.types);
                    for (expected, provided) in iter {
                        if !expected.type_compatible(provided) {
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

#[derive(Debug)]
enum ExecutePreparedState<'a> {
    GetOrPrepare(GetOrPrepare<'a>),
    ExecutePrepared(ExaRoundtrip<ExecutePreparedStmt, SingleResult>),
}

/// Implementor of [`WebSocketFuture`] that executes a batch of SQL statements.
#[derive(Debug)]
pub struct ExecuteBatch<'a>(ExaRoundtrip<request::ExecuteBatch<'a>, MultiResults>);

impl<'a> ExecuteBatch<'a> {
    pub fn new(sql: &'a str) -> Self {
        let request = request::ExecuteBatch(Self::split_query(sql));
        Self(ExaRoundtrip::new(request))
    }

    /// Splits a SQL query into individual statements.
    ///
    /// The splitting follows the following logic:
    /// - trim the query to remove leading and trailing whitespace
    /// - parse each character and store the string slice up to a ';' that is not inside a line or
    ///   block comment and not contained within single or double quotes
    /// - register the next statement start as the next non-whitespace character after a split,
    ///   essentially ignoring whitespace between statements (but retaining comments)
    /// - add the remainder string slice after the last ';' if it is not empty; this means that the
    ///   last statement could be a comment only, but that is okay as Exasol does not complain.
    fn split_query(query: &str) -> Vec<&str> {
        #[derive(Clone, Copy)]
        enum Inside {
            Statement,
            LineComment,
            BlockComment,
            DoubleQuote,
            SingleQuote,
            Whitespace,
        }

        let query = query.trim();
        // NOTE: Using [`char`] as the iterator element for the sake of `char::is_whitespace` which
        //       is more exhaustive than `u8::is_ascii_whitespace`.
        let mut chars = query.char_indices().peekable();
        let mut state = Inside::Statement;
        let mut statements = Vec::new();
        let mut start = 0;

        while let Some((i, c)) = chars.next() {
            let mut peek = || chars.peek().map(|(_, c)| *c);
            let is_whitespace = |p: Option<char>| p.is_some_and(char::is_whitespace);

            #[allow(clippy::match_same_arms, reason = "better readability if split")]
            match (state, c) {
                // Line comment start
                (Inside::Statement, '-') if Some('-') == peek() => {
                    chars.next();
                    state = Inside::LineComment;
                }
                // Block comment start
                (Inside::Statement, '/') if Some('*') == peek() => {
                    chars.next();
                    state = Inside::BlockComment;
                }
                // Double quote start
                (Inside::Statement, '"') => state = Inside::DoubleQuote,
                // Single quote start
                (Inside::Statement, '\'') => state = Inside::SingleQuote,
                // Statement end
                (Inside::Statement, ';') => {
                    statements.push(&query[start..=i]);
                    start = i + 1;

                    // Whitespace between statements start
                    if is_whitespace(peek()) {
                        state = Inside::Whitespace;
                    }
                }
                // Skip escaped double quote
                (Inside::DoubleQuote, '"') if Some('"') == peek() => {
                    chars.next();
                }
                // Skip escaped single quote
                (Inside::SingleQuote, '\'') if Some('\'') == peek() => {
                    chars.next();
                }
                // Double quote end
                (Inside::DoubleQuote, '"') => state = Inside::Statement,
                // Single quote end
                (Inside::SingleQuote, '\'') => state = Inside::Statement,
                // Line comment end
                (Inside::LineComment, '\n') => state = Inside::Statement,
                // Block comment end
                (Inside::BlockComment, '*') if Some('/') == peek() => {
                    chars.next();
                    state = Inside::Statement;
                }
                // Whitespace between statements end
                (Inside::Whitespace, _) if !is_whitespace(peek()) => {
                    start = i + 1;
                    state = Inside::Statement;
                }
                _ => (),
            }
        }

        // Add final part if anything remains after the last `;`.
        // NOTE: Exasol does not complain about trailing comments, but only empty queries.
        let remaining = &query[start..];
        if !remaining.is_empty() {
            statements.push(remaining);
        }

        statements
    }
}

impl WebSocketFuture for ExecuteBatch<'_> {
    type Output = MultiResultStream;

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<SqlxResult<Self::Output>> {
        let multi_results = ready!(self.0.poll_unpin(cx, ws))?;
        Poll::Ready(multi_results.try_into())
    }
}

/// Implementor of [`WebSocketFuture`] that executes a single SQL statement.
#[derive(Debug)]
pub struct Execute<'a>(ExaRoundtrip<request::Execute<'a>, SingleResult>);

impl<'a> Execute<'a> {
    pub fn new(sql: &'a str) -> Self {
        Self(ExaRoundtrip::new(request::Execute(sql.into())))
    }
}

impl WebSocketFuture for Execute<'_> {
    type Output = MultiResultStream;

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<SqlxResult<Self::Output>> {
        self.0.poll_unpin(cx, ws).map_ok(From::from)
    }
}

/// Implementor of [`WebSocketFuture`] that retrieves a prepared statement from the cache, storing
/// it first on cache miss.
#[derive(Debug)]
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

impl WebSocketFuture for GetOrPrepare<'_> {
    type Output = PreparedStatement;

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<SqlxResult<Self::Output>> {
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

#[derive(Debug)]
enum GetOrPrepareState<'a> {
    GetCached,
    CreatePrepared(CreatePrepared<'a>),
    ClosePrepared(ClosePrepared),
}

/// Implementor of [`WebSocketFuture`] that fetches a data chunk from an open result set.
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
    ) -> Poll<SqlxResult<Self::Output>> {
        self.0.poll_unpin(cx, ws)
    }
}

/// Implementor of [`WebSocketFuture`] used to describe a SQL statement. In the lack of native
/// database support, a statement gets prepared and closed right after to retrieve the statement
/// information.
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

impl WebSocketFuture for Describe<'_> {
    type Output = DescribeStatement;

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<SqlxResult<Self::Output>> {
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

/// Implementor of [`WebSocketFuture`] that creates a prepared statement.
#[derive(Debug)]
pub struct CreatePrepared<'a>(ExaRoundtrip<CreatePreparedStmt<'a>, PreparedStatement>);

impl<'a> CreatePrepared<'a> {
    pub fn new(sql: &'a str) -> Self {
        Self(ExaRoundtrip::new(CreatePreparedStmt(sql)))
    }
}

impl WebSocketFuture for CreatePrepared<'_> {
    type Output = PreparedStatement;

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<SqlxResult<Self::Output>> {
        self.0.poll_unpin(cx, ws)
    }
}

/// Implementor of [`WebSocketFuture`] that closes a prepared statement.
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
    ) -> Poll<SqlxResult<Self::Output>> {
        self.0.poll_unpin(cx, ws).map_ok(|_| ())
    }
}

/// Implementor of [`WebSocketFuture`] that closes an array of result sets.
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
    ) -> Poll<SqlxResult<Self::Output>> {
        self.0.poll_unpin(cx, ws).map_ok(|_| ())
    }
}

/// Implementor of [`WebSocketFuture`] that retrieves the IP addresses of the nodes in the Exasol
/// cluser.
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
    ) -> Poll<SqlxResult<Self::Output>> {
        self.0.poll_unpin(cx, ws)
    }
}

/// Implementor of [`WebSocketFuture`] that sets the read-write attributes for the connection.
#[derive(Debug, Default)]
pub struct SetAttributes(ExaRoundtrip<request::SetAttributes, Option<IgnoredAny>>);

impl WebSocketFuture for SetAttributes {
    type Output = ();

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<SqlxResult<Self::Output>> {
        self.0.poll_unpin(cx, ws).map_ok(|_| ())
    }
}

/// Implementor of [`WebSocketFuture`] that retrieves all the read-write and read-only attributes of
/// the connection.
#[derive(Debug, Default)]
pub struct GetAttributes(ExaRoundtrip<request::GetAttributes, Option<IgnoredAny>>);

impl WebSocketFuture for GetAttributes {
    type Output = ();

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<SqlxResult<Self::Output>> {
        self.0.poll_unpin(cx, ws).map_ok(|_| ())
    }
}

/// Implementor of [`WebSocketFuture`] that executes a `COMMIT;` statement.
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
    ) -> Poll<SqlxResult<Self::Output>> {
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

/// Implementor of [`WebSocketFuture`] that executes a `ROLLBACK;` statement.
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
    ) -> Poll<SqlxResult<Self::Output>> {
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

/// Implementor of [`WebSocketFuture`] that disconnects the connection.
#[derive(Default)]
pub struct Disconnect(ExaRoundtrip<request::Disconnect, Option<IgnoredAny>>);

impl WebSocketFuture for Disconnect {
    type Output = ();

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<SqlxResult<Self::Output>> {
        self.0.poll_unpin(cx, ws).map_ok(|_| ())
    }
}

/// Implementor of [`WebSocketFuture`] that authenticates the connection.
///
/// The login process consists of sending the desired login command, optionally receiving some
/// response data from it, and then finishing the login by sending the connection options.
///
/// It is ALWAYS uncompressed.
#[derive(Debug)]
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
    ) -> Poll<SqlxResult<Self::Output>> {
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

#[derive(Debug)]
enum LoginState<'a> {
    Token(ExaRoundtrip<LoginToken, Option<IgnoredAny>>),
    Credentials(ExaRoundtrip<LoginCreds, PublicKey>),
    Complete(ExaRoundtrip<ExaLoginRequest<'a>, SessionInfo>),
}

/// Low-level implementor of [`WebSocketFuture`] that sends a request and awaits its response.
///
/// This type contains logic for graceful handling of pending rollbacks, closing currently open
/// result sets and ignoring pending database responses for futures that were cancelled/dropped.
///
/// All I/O interactions with the database should be built on top of this type.
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

    /// Returns whether the request was successfully sent to the database, even if the response was
    /// not yet received.
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
    ) -> Poll<SqlxResult<Self::Output>> {
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

                    // If there's an active request that we haven't received the response for, await
                    // the response and ignore it.
                    if ws.active_request {
                        ready!(ws.poll_flush_unpin(cx))?;
                        ready!(ws.poll_next_unpin(cx)).transpose()?;
                        ws.active_request = false;
                    }

                    // Wait until we're ready to send a request.
                    ready!(ws.poll_ready_unpin(cx))?;
                    let with_attrs = WithAttributes::new(request, &ws.attributes);
                    let request = serde_json::to_string(&with_attrs)
                        .map_err(|e| SqlxError::Protocol(e.to_string()))?;

                    // Send the request.
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
                    // Get the next response from the database.
                    let Some(bytes) = ready!(ws.poll_next_unpin(cx)).transpose()? else {
                        return Err(ExaProtocolError::from(None::<CloseFrame>))?;
                    };

                    // There's no way to get here unless a request was also sent, yet the sending
                    // logic takes into account whether an active request already exists, therefore
                    // gracefully getting a response means that the current request is no longer
                    // active.
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

#[cfg(test)]
mod tests {
    use super::ExecuteBatch;

    #[test]
    fn test_simple_statements() {
        assert_eq!(
            ExecuteBatch::split_query("SELECT * FROM users; SELECT * FROM orders;"),
            vec!["SELECT * FROM users;", "SELECT * FROM orders;"]
        );
    }

    #[test]
    fn test_semicolon_in_single_quote() {
        assert_eq!(
            ExecuteBatch::split_query("SELECT ';' AS val; SELECT 'abc;def' AS val2;"),
            vec!["SELECT ';' AS val;", "SELECT 'abc;def' AS val2;"]
        );
    }

    #[test]
    fn test_semicolon_in_double_quote() {
        assert_eq!(
            ExecuteBatch::split_query("SELECT \"col;name\" FROM table;"),
            vec!["SELECT \"col;name\" FROM table;"]
        );
    }

    #[test]
    fn test_semicolon_in_line_comment() {
        assert_eq!(
            ExecuteBatch::split_query(
                "SELECT 1; -- this is a comment; with a semicolon\nSELECT 2;"
            ),
            vec![
                "SELECT 1;",
                "-- this is a comment; with a semicolon\nSELECT 2;"
            ]
        );
    }

    #[test]
    fn test_semicolon_in_block_comment() {
        assert_eq!(
            ExecuteBatch::split_query("SELECT 1; /* multi-line ; comment */ SELECT 2;"),
            vec!["SELECT 1;", "/* multi-line ; comment */ SELECT 2;"]
        );
    }

    #[test]
    fn test_escaped_quotes() {
        assert_eq!(
            ExecuteBatch::split_query(
                "SELECT 'It''s a test; really'; SELECT \"escaped\"\"quote\" FROM dual;"
            ),
            vec![
                "SELECT 'It''s a test; really';",
                "SELECT \"escaped\"\"quote\" FROM dual;"
            ]
        );
    }

    #[test]
    fn test_trailing_semicolon_and_whitespace() {
        assert_eq!(
            ExecuteBatch::split_query("SELECT 1;; \n  \n;"),
            vec!["SELECT 1;", ";", ";"]
        );
    }

    #[test]
    fn test_leading_semicolon() {
        assert_eq!(
            ExecuteBatch::split_query(";SELECT 1;"),
            vec![";", "SELECT 1;"]
        );
    }

    #[test]
    fn test_leading_semicolon_and_whitespace() {
        assert_eq!(
            ExecuteBatch::split_query("  ; SELECT 1;"),
            vec![";", "SELECT 1;"]
        );
    }

    #[test]
    fn test_no_semicolon() {
        assert_eq!(ExecuteBatch::split_query("SELECT 1"), vec!["SELECT 1"]);
    }

    #[test]
    fn test_no_whitespace_between_statements() {
        assert_eq!(
            ExecuteBatch::split_query("SELECT 1;SELECT 2"),
            vec!["SELECT 1;", "SELECT 2"]
        );
    }

    #[test]
    fn test_no_whitespace_between_stmt_and_comment() {
        assert_eq!(
            ExecuteBatch::split_query("SELECT 1;/*testing*/SELECT 2;"),
            vec!["SELECT 1;", "/*testing*/SELECT 2;"]
        );
    }

    #[test]
    fn test_trailing_comment() {
        assert_eq!(
            ExecuteBatch::split_query("SELECT 1;/*testing*/"),
            vec!["SELECT 1;", "/*testing*/"]
        );
    }

    #[test]
    fn test_whitespace_between_statements() {
        let query = "
            /* Writing some comments */
            SELECT 1;

            -- Then writing some more comments
            SELECT 2;
            ";
        assert_eq!(
            ExecuteBatch::split_query(query),
            vec![
                "/* Writing some comments */
            SELECT 1;",
                "-- Then writing some more comments
            SELECT 2;"
            ]
        );
    }

    #[test]
    fn test_empty_input() {
        assert_eq!(ExecuteBatch::split_query(""), Vec::<&str>::new());
    }

    #[test]
    fn test_mixed_content() {
        let query = r#"
            SELECT 'test;--'; -- line comment with ;
            /* block comment ;
                over lines */
            SELECT "str;with;semicolons";
        "#;
        assert_eq!(
            ExecuteBatch::split_query(query),
            vec![
                "SELECT 'test;--';",
                r#"-- line comment with ;
            /* block comment ;
                over lines */
            SELECT "str;with;semicolons";"#
            ]
        );
    }
}
