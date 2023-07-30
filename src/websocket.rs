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
    command::{ClosePreparedStmt, CloseResultSet, Command, Fetch, LoginInfo, SetAttributes, Sql},
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

    pub async fn get_results_stream<'a, C, F>(
        &'a mut self,
        command: Command<'_>,
        rs_handle: &mut Option<u16>,
        fetcher_maker: C,
    ) -> Result<QueryResultStream<'_, C, F>, String>
    where
        C: FnMut(&'a mut ExaWebSocket, Fetch) -> F,
        F: Future<Output = Result<(DataChunk, &'a mut ExaWebSocket), String>> + 'a,
    {
        if let Some(handle) = rs_handle {
            self.close_result_set(*handle).await?;
        }

        let query_result = self.get_results(command).await?;
        std::mem::swap(rs_handle, &mut query_result.handle());

        QueryResultStream::new(self, query_result, fetcher_maker)
    }

    pub async fn get_results(&mut self, command: Command<'_>) -> Result<QueryResult, String> {
        let resp_data = self.get_resp_data(command).await?;
        QueryResult::try_from(resp_data)
    }

    pub async fn close_result_set(&mut self, handle: u16) -> Result<(), String> {
        let command = Command::CloseResultSet(CloseResultSet::new(handle));
        self.send_cmd(command).await?;
        Ok(())
    }

    pub async fn create_prepared(
        &mut self,
        command: Command<'_>,
    ) -> Result<PreparedStatement, String> {
        let resp_data = self.get_resp_data(command).await?;
        PreparedStatement::try_from(resp_data)
    }

    pub async fn close_prepared(&mut self, handle: u16) -> Result<(), String> {
        let command = Command::ClosePreparedStatement(ClosePreparedStmt::new(handle));
        self.send_cmd(command).await?;
        Ok(())
    }

    pub async fn fetch_chunk(
        &mut self,
        fetch_cmd: Fetch,
    ) -> Result<(DataChunk, &mut Self), String> {
        let resp_data = self.get_resp_data(Command::Fetch(fetch_cmd)).await?;
        DataChunk::try_from(resp_data).map(|c| (c, self))
    }

    pub async fn set_attributes(&mut self) -> Result<(), String> {
        let command = Command::SetAttributes(SetAttributes::new(&self.attributes));
        let str_cmd = serde_json::to_string(&command).map_err(|e| e.to_string())?;
        self.send_raw_cmd(str_cmd).await?;
        Ok(())
    }

    pub async fn get_attributes(&mut self) -> Result<(), String> {
        self.send_cmd(Command::GetAttributes).await?;
        Ok(())
    }

    pub async fn begin(&mut self) -> Result<(), String> {
        if self.attributes.open_transaction {
            return Err("Transaction already open!".to_owned());
        }

        self.attributes.autocommit = false;
        self.set_attributes().await
    }

    pub async fn commit(&mut self) -> Result<(), String> {
        self.send_cmd(Command::Execute(Sql::new("COMMIT;"))).await?;
        self.attributes.autocommit = true;
        self.set_attributes().await?;
        Ok(())
    }

    pub async fn rollback(&mut self) -> Result<(), String> {
        self.send_cmd(Command::Execute(Sql::new("ROLLBACK;")))
            .await?;
        self.attributes.autocommit = true;
        self.set_attributes().await?;
        Ok(())
    }

    pub async fn ping(&mut self) -> Result<(), String> {
        self.ws
            .send(Message::Ping(Vec::new()))
            .await
            .map_err(|e| e.to_string())?;

        Ok(())
    }

    pub async fn disconnect(&mut self) -> Result<(), String> {
        self.send_cmd(Command::Disconnect).await?;
        Ok(())
    }

    pub async fn close(&mut self) -> Result<(), String> {
        self.ws.close(None).await.map_err(|e| e.to_string())?;
        Ok(())
    }

    pub async fn get_or_prepare<'a>(
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

        let command = Sql::new(sql);
        let prepared = self
            .create_prepared(Command::CreatePreparedStatement(command))
            .await?;

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

        let str_cmd = serde_json::to_string(&opts).map_err(|e| e.to_string())?;
        self.send_raw_cmd(str_cmd).await?;
        Ok(())
    }

    #[cfg(feature = "migrate")]
    pub(crate) async fn execute_batch(&mut self, sql: &str) -> Result<(), String> {
        use crate::command::BatchSql;

        let sql = sql.trim_end();
        let sql = sql.strip_suffix(';').unwrap_or(sql);

        let command = BatchSql::new(sql.split(';').collect());

        if self.send_cmd(Command::ExecuteBatch(command)).await.is_ok() {
            return Ok(());
        }

        let result = Ok(());
        let mut position = 0;
        let mut sql_start = 0;

        while let Some(sql_end) = sql[position..].find(';') {
            let sql = &sql[sql_start..sql_end];
            let command = Sql::new(sql);
            let command = Command::Execute(command);

            if let Err(_e) = self.send_cmd(command).await {
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
        let command = Command::LoginToken(LoginInfo::new(protocol_version));
        self.send_cmd(command).await?;
        Ok(())
    }

    async fn get_pub_key(
        &mut self,
        protocol_version: ProtocolVersion,
    ) -> Result<RsaPublicKey, String> {
        let command = Command::Login(LoginInfo::new(protocol_version));
        let resp_data = self.get_resp_data(command).await?;
        RsaPublicKey::try_from(resp_data)
    }

    async fn get_resp_data(&mut self, command: Command<'_>) -> Result<ResponseData, String> {
        self.send_cmd(command)
            .await?
            .ok_or_else(|| "No response data received".to_owned())
    }

    async fn send_cmd(&mut self, command: Command<'_>) -> Result<Option<ResponseData>, String> {
        let str_cmd = serde_json::to_string(&command).map_err(|e| e.to_string())?;
        self.send_raw_cmd(str_cmd).await
    }

    async fn send_raw_cmd(&mut self, str_cmd: String) -> Result<Option<ResponseData>, String> {
        #[allow(unreachable_patterns)]
        let response = match self.attributes.compression_enabled {
            false => self.send_uncompressed_cmd(str_cmd.clone()).await?,
            #[cfg(feature = "flate2")]
            true => self.send_compressed_cmd(str_cmd.clone()).await?,
            _ => return Err("feature 'flate2' must be enabled to use compression".to_owned()),
        };

        let (response_data, attributes) = match response {
            Response::Ok {
                response_data,
                attributes,
            } => (response_data, attributes),
            Response::Error { exception } => return Err(format!("{str_cmd}-{exception}")),
        };

        if let Some(attributes) = attributes {
            self.attributes.update(attributes)
        }

        Ok(response_data)
    }

    async fn send_uncompressed_cmd(&mut self, str_cmd: String) -> Result<Response, String> {
        self.ws
            .send(Message::Text(str_cmd))
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
    async fn send_compressed_cmd(&mut self, msg_string: String) -> Result<Response, String> {
        use std::io::Write;

        use flate2::{read::ZlibDecoder, write::ZlibEncoder, Compression};

        let msg = msg_string.as_bytes();
        let mut buf = Vec::new();
        let mut enc = ZlibEncoder::new(&mut buf, Compression::default());

        enc.write_all(msg)
            .and_then(|_| enc.finish())
            .map_err(|e| e.to_string())?;

        self.ws
            .send(Message::Binary(buf))
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
