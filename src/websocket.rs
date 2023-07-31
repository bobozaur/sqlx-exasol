use std::{borrow::Cow, fmt::Debug, io, task::Poll};

use async_tungstenite::{tungstenite::Message, WebSocketStream};
use futures_io::{AsyncRead, AsyncWrite};
use futures_util::{io::BufReader, Future, SinkExt, StreamExt};
use lru::LruCache;
use rsa::RsaPublicKey;
use sqlx_core::{
    bytes::BufMut,
    net::{Socket, WithSocket},
};

use crate::{
    command::{Command, ExaCommand},
    options::{
        login::{CredentialsRef, LoginRef},
        ExaConnectOptionsRef, ProtocolVersion,
    },
    responses::{
        fetched::DataChunk, prepared_stmt::PreparedStatement, result::QueryResult, ExaAttributes,
        Response, ResponseData,
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
    ) -> Result<Self, String> {
        let (socket, is_tls) = tls::maybe_upgrade(socket.0, host, options.clone()).await?;

        let scheme = match is_tls {
            true => Self::WSS_SCHEME,
            false => Self::WS_SCHEME,
        };

        let host = format!("{scheme}://{host}");

        let (ws, _) = async_tungstenite::client_async(host, BufReader::new(socket))
            .await
            .map_err(|e| e.to_string())?;

        let mut ws = Self {
            ws,
            attributes: Default::default(),
        };

        ws.attributes.encryption_enabled = is_tls;
        ws.attributes.fetch_size = options.fetch_size;
        ws.attributes.statement_cache_capacity = options.statement_cache_capacity;

        ws.login(options).await?;
        ws.get_attributes().await?;

        Ok(ws)
    }

    pub(crate) async fn get_results_stream<'a, C, F>(
        &'a mut self,
        cmd: Command,
        rs_handle: &mut Option<u16>,
        fetcher_maker: C,
    ) -> Result<QueryResultStream<'_, C, F>, String>
    where
        C: FnMut(&'a mut ExaWebSocket, Command) -> F,
        F: Future<Output = Result<(DataChunk, &'a mut ExaWebSocket), String>> + 'a,
    {
        if let Some(handle) = rs_handle {
            self.close_result_set(*handle).await?;
        }

        let query_result = self.get_results(cmd).await?;
        std::mem::swap(rs_handle, &mut query_result.handle());

        QueryResultStream::new(self, query_result, fetcher_maker)
    }

    pub(crate) async fn get_results(&mut self, cmd: Command) -> Result<QueryResult, String> {
        let resp_data = self.get_resp_data(cmd).await?;
        QueryResult::try_from(resp_data)
    }

    pub(crate) async fn close_result_set(&mut self, handle: u16) -> Result<(), String> {
        let cmd = ExaCommand::new_close_result(handle).try_into()?;
        self.send_cmd(cmd).await?;
        Ok(())
    }

    pub(crate) async fn create_prepared(
        &mut self,
        cmd: Command,
    ) -> Result<PreparedStatement, String> {
        let resp_data = self.get_resp_data(cmd).await?;
        PreparedStatement::try_from(resp_data)
    }

    pub(crate) async fn close_prepared(&mut self, handle: u16) -> Result<(), String> {
        let cmd = ExaCommand::new_close_prepared(handle).try_into()?;
        self.send_cmd(cmd).await?;
        Ok(())
    }

    pub(crate) async fn fetch_chunk(
        &mut self,
        cmd: Command,
    ) -> Result<(DataChunk, &mut Self), String> {
        let resp_data = self.get_resp_data(cmd).await?;
        DataChunk::try_from(resp_data).map(|c| (c, self))
    }

    #[allow(dead_code)]
    pub(crate) async fn set_attributes(&mut self) -> Result<(), String> {
        let cmd = ExaCommand::new_set_attributes(&self.attributes).try_into()?;
        self.send_cmd(cmd).await?;

        Ok(())
    }

    pub(crate) async fn get_attributes(&mut self) -> Result<(), String> {
        let cmd = ExaCommand::GetAttributes.try_into()?;
        self.send_cmd(cmd).await?;
        Ok(())
    }

    pub(crate) async fn begin(&mut self) -> Result<(), String> {
        // Exasol does not have nested transactions.
        if self.attributes.open_transaction {
            return Err("Transaction already open!".to_owned());
        }

        // The next time a query is executed, the transaction will be started.
        // We could eagerly start it as well, but that implies one more
        // round-trip to the server and back with no benefit.
        self.attributes.autocommit = false;
        Ok(())
    }

    pub(crate) async fn commit(&mut self) -> Result<(), String> {
        self.attributes.autocommit = true;

        // Just changing `autocommit` attribute implies a COMMIT,
        // but we would still have to send a command to the server
        // to update it, so we might as well be explicit.
        let cmd = ExaCommand::new_execute("COMMIT;", &self.attributes).try_into()?;
        self.send_cmd(cmd).await?;

        Ok(())
    }

    pub(crate) async fn rollback(&mut self) -> Result<(), String> {
        self.attributes.autocommit = true;

        let cmd = ExaCommand::new_execute("ROLLBACK;", &self.attributes).try_into()?;
        self.send_cmd(cmd).await?;

        Ok(())
    }

    pub(crate) async fn ping(&mut self) -> Result<(), String> {
        self.ws
            .send(Message::Ping(Vec::new()))
            .await
            .map_err(|e| e.to_string())?;

        Ok(())
    }

    pub(crate) async fn disconnect(&mut self) -> Result<(), String> {
        let cmd = ExaCommand::Disconnect.try_into()?;
        self.send_cmd(cmd).await?;
        Ok(())
    }

    pub(crate) async fn close(&mut self) -> Result<(), String> {
        self.ws.close(None).await.map_err(|e| e.to_string())?;
        Ok(())
    }

    pub(crate) async fn get_or_prepare<'a>(
        &mut self,
        cache: &'a mut LruCache<String, PreparedStatement>,
        sql: &str,
        persist: bool,
    ) -> Result<Cow<'a, PreparedStatement>, String> {
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

    pub(crate) async fn login(&mut self, mut opts: ExaConnectOptionsRef<'_>) -> Result<(), String> {
        match &mut opts.login {
            LoginRef::Credentials(creds) => {
                self.start_login_credentials(creds, opts.protocol_version)
                    .await?
            }
            _ => self.start_login_token(opts.protocol_version).await?,
        }

        let cmd = (&opts).try_into()?;
        self.send_cmd(cmd).await?;
        Ok(())
    }

    #[cfg(feature = "migrate")]
    pub(crate) async fn execute_batch(&mut self, sql: &str) -> Result<(), String> {
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
    ) -> Result<(), String> {
        let key = self.get_pub_key(protocol_version).await?;
        credentials
            .encrypt_password(key)
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn start_login_token(&mut self, protocol_version: ProtocolVersion) -> Result<(), String> {
        let cmd = ExaCommand::new_login_token(protocol_version).try_into()?;
        self.send_cmd(cmd).await?;
        Ok(())
    }

    async fn get_pub_key(
        &mut self,
        protocol_version: ProtocolVersion,
    ) -> Result<RsaPublicKey, String> {
        let cmd = ExaCommand::new_login(protocol_version).try_into()?;
        let resp_data = self.get_resp_data(cmd).await?;
        RsaPublicKey::try_from(resp_data)
    }

    async fn get_resp_data(&mut self, cmd: Command) -> Result<ResponseData, String> {
        self.send_cmd(cmd)
            .await?
            .ok_or_else(|| "No response data received".to_owned())
    }

    async fn send_cmd(&mut self, cmd: Command) -> Result<Option<ResponseData>, String> {
        let cmd = cmd.into_inner();
        tracing::trace!("Sending command to database: {cmd}");

        #[allow(unreachable_patterns)]
        let response = match self.attributes.compression_enabled {
            false => self.send_uncompressed_cmd(cmd).await?,
            #[cfg(feature = "flate2")]
            true => self.send_compressed_cmd(cmd).await?,
            _ => return Err("feature 'flate2' must be enabled to use compression".to_owned()),
        };

        let (response_data, attributes) = match response {
            Response::Ok {
                response_data,
                attributes,
            } => (response_data, attributes),
            Response::Error { exception } => return Err(exception.to_string()),
        };

        if let Some(attributes) = attributes {
            tracing::trace!("Updating connection attributes using: {attributes:?}");
            self.attributes.update(attributes)
        }

        Ok(response_data)
    }

    async fn send_uncompressed_cmd(&mut self, cmd: String) -> Result<Response, String> {
        self.ws
            .send(Message::Text(cmd))
            .await
            .map_err(|e| format!("Error sending: {e}"))?;

        while let Some(response) = self.ws.next().await {
            let msg = response.map_err(|e| e.to_string())?;

            return match msg {
                Message::Text(s) => serde_json::from_str(&s).map_err(|e| format!("{s} - {e}")),
                Message::Binary(v) => serde_json::from_slice(&v).map_err(|e| e.to_string()),
                Message::Close(c) => Err(format!("Close frame received: {c:?}")),
                _ => continue,
            };
        }

        Err("No message received".to_owned())
    }

    #[cfg(feature = "flate2")]
    async fn send_compressed_cmd(&mut self, cmd: String) -> Result<Response, String> {
        use std::io::Write;

        use flate2::{read::ZlibDecoder, write::ZlibEncoder, Compression};

        let byte_cmd = cmd.as_bytes();
        let mut compressed_cmd = Vec::new();
        let mut enc = ZlibEncoder::new(&mut compressed_cmd, Compression::default());

        enc.write_all(byte_cmd)
            .and_then(|_| enc.finish())
            .map_err(|e| e.to_string())?;

        self.ws
            .send(Message::Binary(compressed_cmd))
            .await
            .map_err(|e| e.to_string())?;

        while let Some(response) = self.ws.next().await {
            let bytes = match response.map_err(|e| e.to_string())? {
                Message::Text(s) => s.into_bytes(),
                Message::Binary(v) => v,
                Message::Close(c) => return Err(format!("Close frame received: {c:?}")),
                _ => continue,
            };

            let dec = ZlibDecoder::new(bytes.as_slice());
            return serde_json::from_reader(dec).map_err(|e| e.to_string());
        }

        Err("No message received".to_owned())
    }
}
pub struct WithRwSocket;

impl WithSocket for WithRwSocket {
    type Output = RwSocket;

    fn with_socket<S: Socket>(self, socket: S) -> Self::Output {
        RwSocket(Box::new(socket))
    }
}

pub struct RwSocket(Box<dyn Socket>);

impl Debug for RwSocket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", stringify!(RwSocket))
    }
}

impl AsyncRead for RwSocket {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        mut buf: &mut [u8],
    ) -> std::task::Poll<futures_io::Result<usize>> {
        while buf.has_remaining_mut() {
            match self.0.try_read(&mut buf) {
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    let Poll::Ready(_) = self.0.poll_read_ready(cx)? else {
                        return Poll::Pending;
                    };
                }
                ready => return Poll::Ready(ready),
            }
        }

        Poll::Ready(Ok(0))
    }
}

impl AsyncWrite for RwSocket {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<futures_io::Result<usize>> {
        while !buf.is_empty() {
            match self.0.try_write(buf) {
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    let Poll::Ready(_) = self.0.poll_write_ready(cx)? else {
                        return Poll::Pending;
                    };
                }
                ready => return Poll::Ready(ready),
            }
        }

        Poll::Ready(Ok(0))
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<futures_io::Result<()>> {
        self.0.poll_flush(cx)
    }

    fn poll_close(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<futures_io::Result<()>> {
        self.0.poll_shutdown(cx)
    }
}
