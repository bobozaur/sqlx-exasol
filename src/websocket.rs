use std::{borrow::Cow, fmt::Debug, io, task::Poll};

use async_tungstenite::{tungstenite::Message, WebSocketStream};
use futures_io::{AsyncRead, AsyncWrite};
use futures_util::{Future, SinkExt, StreamExt};
use lru::LruCache;
use rsa::{pkcs1::DecodeRsaPublicKey, RsaPublicKey};
use sqlx_core::{
    bytes::BufMut,
    net::{Socket, WithSocket},
};

use crate::{
    command::{
        ClosePreparedStmt, CloseResultSet, Command, Fetch, LoginInfo, SetAttributes, SqlText,
    },
    con_opts::{
        login::{CredentialsRef, LoginRef},
        ExaConnectOptionsRef, ProtocolVersion,
    },
    responses::{
        fetched::DataChunk, prepared_stmt::PreparedStatement, result::QueryResult, Attributes,
        Response, ResponseData,
    },
    stream::QueryResultStream,
};

#[derive(Debug)]
pub struct ExaWebSocket {
    attributes: Attributes,
    pub ws: WebSocketStream<RwSocket>,
}

impl ExaWebSocket {
    pub(crate) async fn new(
        host: &str,
        socket: RwSocket,
        opts: ExaConnectOptionsRef<'_>,
    ) -> Result<Self, String> {
        let (ws, _) = async_tungstenite::client_async(host, socket)
            .await
            .map_err(|e| e.to_string())?;

        let mut ws = Self {
            attributes: Default::default(),
            ws,
        };

        ws.login(opts).await?;
        ws.get_attributes().await?;

        Ok(ws)
    }

    pub async fn get_results_stream<'a, C, F>(
        &'a mut self,
        command: Command<'_>,
        rs_handle: &mut Option<u16>,
        fetch_maker: C,
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

        QueryResultStream::new(self, query_result, fetch_maker)
    }

    pub async fn get_results(&mut self, command: Command<'_>) -> Result<QueryResult, String> {
        let resp_data = self.get_resp_data(command).await?;

        match resp_data {
            ResponseData::Results(r) => Ok(r.results.into_iter().next().unwrap()),
            _ => Err("Expected results response".to_owned()),
        }
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

        match resp_data {
            ResponseData::PreparedStatement(p) => Ok(p),
            _ => Err("Expected prepared statement response".to_owned()),
        }
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

        match resp_data {
            ResponseData::FetchedData(f) => Ok((f, self)),
            _ => Err("Expected fetched data response".to_owned()),
        }
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
        if self.attributes.autocommit {
            return Err("Transaction already open!".to_owned());
        }

        self.attributes.autocommit = false;
        self.set_attributes().await
    }

    pub async fn commit(&mut self) -> Result<(), String> {
        self.send_cmd(Command::Execute(SqlText::new("COMMIT;")))
            .await?;
        self.attributes.autocommit = false;
        self.set_attributes().await?;
        Ok(())
    }

    pub async fn rollback(&mut self) -> Result<(), String> {
        self.send_cmd(Command::Execute(SqlText::new("ROLLBACK;")))
            .await?;
        self.attributes.autocommit = false;
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

        let command = SqlText::new(sql);
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

        match resp_data {
            ResponseData::PublicKey(key) => {
                RsaPublicKey::from_pkcs1_pem(&key.public_key_pem).map_err(|e| e.to_string())
            }
            _ => Err("Expected public key response".to_owned()),
        }
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
        let response = self.send_uncompressed_cmd(str_cmd).await?;

        match response {
            Response::Ok {
                response_data,
                attributes,
            } => {
                if let Some(attributes) = attributes {
                    self.attributes = attributes;
                }

                Ok(response_data)
            }
            Response::Error { exception } => Err(exception.to_string()),
        }
    }

    async fn send_uncompressed_cmd(&mut self, str_cmd: String) -> Result<Response, String> {
        self.ws
            .send(Message::Text(str_cmd))
            .await
            .map_err(|e| e.to_string())?;

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

    // #[cfg(feature = "flate2")]
    // async fn send_compressed_cmd(&mut self, msg_string: String) {
    //     let msg = msg_string.as_bytes();
    //     let mut buf = Vec::new();
    //     ZlibEncoder::new(msg, Compression::default()).read_to_end(&mut buf);
    //     self.ws.send(Message::Binary(buf)).await.unwrap();

    //     while let Some(response) = self.ws.next().await {
    //         return match response.unwrap() {
    //             Message::Text(s) => serde_json::from_reader(ZlibDecoder::new(s.as_bytes())),
    //             Message::Binary(v) => serde_json::from_reader(ZlibDecoder::new(v.as_slice())),
    //             Message::Close(c) => (),
    //             _ => continue,
    //         };
    //     }
    // }
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
                    let Poll::Ready(_) = self.0.poll_read_ready(cx)? else {return Poll::Pending};
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
                    let Poll::Ready(_) = self.0.poll_write_ready(cx)? else {return Poll::Pending};
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
