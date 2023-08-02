use std::{
    borrow::Cow,
    fmt::Debug,
    io,
    pin::Pin,
    task::{Context, Poll},
};

use async_tungstenite::{tungstenite::Message, WebSocketStream};
use futures_core::ready;
use futures_io::{AsyncRead, AsyncWrite};
use futures_util::{io::BufReader, Future, SinkExt, StreamExt};
use lru::LruCache;
use rsa::RsaPublicKey;
use sqlx_core::{
    bytes::BufMut,
    net::{Socket, WithSocket},
    Error as SqlxError,
};

use crate::{
    command::{Command, ExaCommand},
    error::{ExaProtocolError, ExaResultExt},
    options::{
        ExaConnectOptionsRef, ProtocolVersion, {CredentialsRef, LoginRef},
    },
    responses::{
        DataChunk, ExaAttributes, PreparedStatement, QueryResult, Response, ResponseData,
        SessionInfo,
    },
    stream::QueryResultStream,
    tls,
};

#[derive(Debug)]
pub struct ExaWebSocket {
    pub(crate) ws: WebSocketStream<BufReader<RwSocket>>,
    pub(crate) attributes: ExaAttributes,
}

impl ExaWebSocket {
    const WS_SCHEME: &str = "ws";
    const WSS_SCHEME: &str = "wss";

    pub(crate) async fn new(
        host: &str,
        socket: RwSocket,
        options: ExaConnectOptionsRef<'_>,
    ) -> Result<(Self, SessionInfo), SqlxError> {
        let (socket, is_tls) = tls::maybe_upgrade(socket.0, host, options.clone()).await?;

        let scheme = match is_tls {
            true => Self::WSS_SCHEME,
            false => Self::WS_SCHEME,
        };

        let host = format!("{scheme}://{host}");

        let (ws, _) = async_tungstenite::client_async(host, BufReader::new(socket))
            .await
            .to_sqlx_err()?;

        let mut ws = Self {
            ws,
            attributes: Default::default(),
        };

        ws.attributes.encryption_enabled = is_tls;
        ws.attributes.fetch_size = options.fetch_size;
        ws.attributes.statement_cache_capacity = options.statement_cache_capacity;

        let session_info = ws.login(options).await?;
        ws.get_attributes().await?;

        Ok((ws, session_info))
    }

    pub(crate) async fn get_results_stream<'a, C, F>(
        &'a mut self,
        cmd: Command,
        rs_handle: &mut Option<u16>,
        fetcher_maker: C,
    ) -> Result<QueryResultStream<'_, C, F>, SqlxError>
    where
        C: FnMut(&'a mut ExaWebSocket, Command) -> F,
        F: Future<Output = Result<(DataChunk, &'a mut ExaWebSocket), SqlxError>> + 'a,
    {
        if let Some(handle) = rs_handle {
            self.close_result_set(*handle).await?;
        }

        let query_result = self.get_results(cmd).await?;
        std::mem::swap(rs_handle, &mut query_result.handle());

        QueryResultStream::new(self, query_result, fetcher_maker)
    }

    pub(crate) async fn get_results(&mut self, cmd: Command) -> Result<QueryResult, SqlxError> {
        let resp_data = self.get_resp_data(cmd).await?;
        QueryResult::try_from(resp_data)
    }

    pub(crate) async fn close_result_set(&mut self, handle: u16) -> Result<(), SqlxError> {
        let cmd = ExaCommand::new_close_result(handle).try_into()?;
        self.send_cmd(cmd).await?;
        Ok(())
    }

    pub(crate) async fn create_prepared(
        &mut self,
        cmd: Command,
    ) -> Result<PreparedStatement, SqlxError> {
        let resp_data = self.get_resp_data(cmd).await?;
        PreparedStatement::try_from(resp_data)
    }

    pub(crate) async fn close_prepared(&mut self, handle: u16) -> Result<(), SqlxError> {
        let cmd = ExaCommand::new_close_prepared(handle).try_into()?;
        self.send_cmd(cmd).await?;
        Ok(())
    }

    pub(crate) async fn fetch_chunk(
        &mut self,
        cmd: Command,
    ) -> Result<(DataChunk, &mut Self), SqlxError> {
        let resp_data = self.get_resp_data(cmd).await?;
        DataChunk::try_from(resp_data).map(|c| (c, self))
    }

    #[allow(dead_code)]
    pub(crate) async fn set_attributes(&mut self) -> Result<(), SqlxError> {
        let cmd = ExaCommand::new_set_attributes(&self.attributes).try_into()?;
        self.send_cmd(cmd).await?;

        Ok(())
    }

    pub(crate) async fn get_attributes(&mut self) -> Result<(), SqlxError> {
        let cmd = ExaCommand::GetAttributes.try_into()?;
        self.send_cmd(cmd).await?;
        Ok(())
    }

    pub(crate) async fn begin(&mut self) -> Result<(), SqlxError> {
        // Exasol does not have nested transactions.
        if self.attributes.open_transaction {
            return Err(ExaProtocolError::TransactionAlreadyOpen)?;
        }

        // The next time a query is executed, the transaction will be started.
        // We could eagerly start it as well, but that implies one more
        // round-trip to the server and back with no benefit.
        self.attributes.autocommit = false;
        Ok(())
    }

    pub(crate) async fn commit(&mut self) -> Result<(), SqlxError> {
        self.attributes.autocommit = true;

        // Just changing `autocommit` attribute implies a COMMIT,
        // but we would still have to send a command to the server
        // to update it, so we might as well be explicit.
        let cmd = ExaCommand::new_execute("COMMIT;", &self.attributes).try_into()?;
        self.send_cmd(cmd).await?;

        Ok(())
    }

    pub(crate) async fn rollback(&mut self) -> Result<(), SqlxError> {
        self.attributes.autocommit = true;

        let cmd = ExaCommand::new_execute("ROLLBACK;", &self.attributes).try_into()?;
        self.send_cmd(cmd).await?;

        Ok(())
    }

    pub(crate) async fn ping(&mut self) -> Result<(), SqlxError> {
        self.ws
            .send(Message::Ping(Vec::new()))
            .await
            .to_sqlx_err()?;

        Ok(())
    }

    pub(crate) async fn disconnect(&mut self) -> Result<(), SqlxError> {
        let cmd = ExaCommand::Disconnect.try_into()?;
        self.send_cmd(cmd).await?;
        Ok(())
    }

    pub(crate) async fn close(&mut self) -> Result<(), SqlxError> {
        self.ws.close(None).await.to_sqlx_err()?;
        Ok(())
    }

    pub(crate) async fn get_or_prepare<'a>(
        &mut self,
        cache: &'a mut LruCache<String, PreparedStatement>,
        sql: &str,
        persist: bool,
    ) -> Result<Cow<'a, PreparedStatement>, SqlxError> {
        // The double look-up is required to avoid a borrow checker limitation.
        //
        // See: https://github.com/rust-lang/rust/issues/54663
        if cache.contains(sql) {
            return Ok(Cow::Borrowed(cache.get(sql).unwrap()));
        }

        let cmd = ExaCommand::new_create_prepared(sql).try_into()?;
        let prepared = self.create_prepared(cmd).await?;

        if persist {
            if let Some(old) = cache.put(sql.to_owned(), prepared) {
                self.close_prepared(old.statement_handle).await?;
            }

            return Ok(Cow::Borrowed(cache.get(sql).unwrap()));
        }

        Ok(Cow::Owned(prepared))
    }

    pub(crate) async fn login(
        &mut self,
        mut opts: ExaConnectOptionsRef<'_>,
    ) -> Result<SessionInfo, SqlxError> {
        match &mut opts.login {
            LoginRef::Credentials(creds) => {
                self.start_login_credentials(creds, opts.protocol_version)
                    .await?
            }
            _ => self.start_login_token(opts.protocol_version).await?,
        }

        let cmd = (&opts).try_into()?;
        self.get_session_info(cmd).await
    }

    #[cfg(feature = "migrate")]
    pub(crate) async fn execute_batch(&mut self, sql: &str) -> Result<(), SqlxError> {
        let sql = sql.trim_end();
        let sql = sql.strip_suffix(';').unwrap_or(sql);

        let sql_batch = sql.split(';').collect();
        let cmd = ExaCommand::new_execute_batch(sql_batch, &self.attributes).try_into()?;

        if self.send_cmd(cmd).await.is_ok() {
            return Ok(());
        }

        let result = Ok(());
        let mut position = 0;
        let mut sql_start = 0;

        while let Some(sql_end) = sql[position..].find(';') {
            let sql = &sql[sql_start..sql_end];
            let cmd = ExaCommand::new_execute(sql, &self.attributes).try_into()?;

            if let Err(_e) = self.send_cmd(cmd).await {
                position = sql.len();
                // TODO: match on e after proper error handling
            } else {
                position = sql.len();
                sql_start = position;
            }
        }

        result
    }

    async fn start_login_credentials(
        &mut self,
        credentials: &mut CredentialsRef<'_>,
        protocol_version: ProtocolVersion,
    ) -> Result<(), SqlxError> {
        let key = self.get_pub_key(protocol_version).await?;
        credentials.encrypt_password(key)?;

        Ok(())
    }

    async fn start_login_token(
        &mut self,
        protocol_version: ProtocolVersion,
    ) -> Result<(), SqlxError> {
        let cmd = ExaCommand::new_login_token(protocol_version).try_into()?;
        self.send_cmd(cmd).await?;
        Ok(())
    }

    async fn get_pub_key(
        &mut self,
        protocol_version: ProtocolVersion,
    ) -> Result<RsaPublicKey, SqlxError> {
        let cmd = ExaCommand::new_login(protocol_version).try_into()?;
        let resp_data = self.get_resp_data(cmd).await?;
        RsaPublicKey::try_from(resp_data)
    }

    async fn get_session_info(&mut self, cmd: Command) -> Result<SessionInfo, SqlxError> {
        let resp_data = self.get_resp_data(cmd).await?;
        SessionInfo::try_from(resp_data)
    }

    async fn get_resp_data(&mut self, cmd: Command) -> Result<ResponseData, SqlxError> {
        self.send_cmd(cmd)
            .await?
            .ok_or(ExaProtocolError::MissingResponseData)
            .map_err(From::from)
    }

    async fn send_cmd(&mut self, cmd: Command) -> Result<Option<ResponseData>, SqlxError> {
        let cmd = cmd.into_inner();
        tracing::trace!("Sending command to database: {cmd}");

        #[allow(unreachable_patterns)]
        let response = match self.attributes.compression_enabled {
            false => self.send_uncompressed_cmd(cmd).await?,
            #[cfg(feature = "flate2")]
            true => self.send_compressed_cmd(cmd).await?,
            _ => return Err(ExaProtocolError::CompressionDisabled)?,
        };

        let (response_data, attributes) = match response {
            Response::Ok {
                response_data,
                attributes,
            } => (response_data, attributes),
            Response::Error { exception } => return Err(exception)?,
        };

        if let Some(attributes) = attributes {
            tracing::trace!("Updating connection attributes using: {attributes:?}");
            self.attributes.update(attributes)
        }

        tracing::trace!("Database response: {response_data:?}");

        Ok(response_data)
    }

    async fn send_uncompressed_cmd(&mut self, cmd: String) -> Result<Response, SqlxError> {
        self.ws.send(Message::Text(cmd)).await.to_sqlx_err()?;

        while let Some(response) = self.ws.next().await {
            let msg = response.to_sqlx_err()?;

            return match msg {
                Message::Text(s) => serde_json::from_str(&s).to_sqlx_err(),
                Message::Binary(v) => serde_json::from_slice(&v).to_sqlx_err(),
                Message::Close(c) => {
                    self.close().await.ok();
                    Err(ExaProtocolError::from(c))?
                }
                _ => continue,
            };
        }

        Err(ExaProtocolError::MissingMessage)?
    }

    #[cfg(feature = "flate2")]
    async fn send_compressed_cmd(&mut self, cmd: String) -> Result<Response, SqlxError> {
        use std::io::Write;

        use flate2::{read::ZlibDecoder, write::ZlibEncoder, Compression};

        let byte_cmd = cmd.as_bytes();
        let mut compressed_cmd = Vec::new();
        let mut enc = ZlibEncoder::new(&mut compressed_cmd, Compression::default());

        enc.write_all(byte_cmd).and_then(|_| enc.finish())?;

        self.ws
            .send(Message::Binary(compressed_cmd))
            .await
            .to_sqlx_err()?;

        while let Some(response) = self.ws.next().await {
            let bytes = match response.to_sqlx_err()? {
                Message::Text(s) => s.into_bytes(),
                Message::Binary(v) => v,
                Message::Close(c) => {
                    self.close().await.ok();
                    Err(ExaProtocolError::from(c))?
                }
                _ => continue,
            };

            let dec = ZlibDecoder::new(bytes.as_slice());
            return serde_json::from_reader(dec).to_sqlx_err();
        }

        Err(ExaProtocolError::MissingMessage)?
    }
}

/// Implementor of [`WithSocket`].
pub struct WithRwSocket;

impl WithSocket for WithRwSocket {
    type Output = RwSocket;

    fn with_socket<S: Socket>(self, socket: S) -> Self::Output {
        RwSocket(Box::new(socket))
    }
}

/// A wrapper so we can implement [`AsyncRead`] and [`AsyncWrite`]
/// for the underlying TCP socket. The traits are needed by the
/// [`WebSocketStream`] wrapper.
pub struct RwSocket(Box<dyn Socket>);

impl Debug for RwSocket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", stringify!(RwSocket))
    }
}

impl AsyncRead for RwSocket {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut buf: &mut [u8],
    ) -> Poll<futures_io::Result<usize>> {
        while buf.has_remaining_mut() {
            match self.0.try_read(&mut buf) {
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    ready!(self.0.poll_read_ready(cx)?);
                }
                ready => return Poll::Ready(ready),
            }
        }

        Poll::Ready(Ok(0))
    }
}

impl AsyncWrite for RwSocket {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<futures_io::Result<usize>> {
        while !buf.is_empty() {
            match self.0.try_write(buf) {
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    ready!(self.0.poll_write_ready(cx)?)
                }
                ready => return Poll::Ready(ready),
            }
        }

        Poll::Ready(Ok(0))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<futures_io::Result<()>> {
        self.0.poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<futures_io::Result<()>> {
        self.0.poll_shutdown(cx)
    }
}
