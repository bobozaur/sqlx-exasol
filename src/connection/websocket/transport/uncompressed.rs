use std::{
    pin::Pin,
    task::{ready, Context, Poll},
};

use async_tungstenite::{tungstenite::Message, WebSocketStream};
use bytes::Bytes;
use futures_core::Stream;
use futures_util::{io::BufReader, Sink, SinkExt, StreamExt, TryStreamExt};
use rsa::RsaPublicKey;
use serde::de::{DeserializeOwned, IgnoredAny};
use sqlx_core::Error as SqlxError;

use crate::{
    command::ExaCommand,
    connection::websocket::socket::ExaSocket,
    error::{ExaProtocolError, ToSqlxError},
    options::{CredentialsRef, ExaConnectOptionsRef, LoginRef},
    responses::{Attributes, PublicKey, Response, SessionInfo},
    ProtocolVersion,
};

#[derive(Debug)]
pub struct PlainWebSocket(pub WebSocketStream<BufReader<ExaSocket>>);

impl PlainWebSocket {
    pub async fn new(
        ws: WebSocketStream<BufReader<ExaSocket>>,
        opts: ExaConnectOptionsRef<'_>,
    ) -> Result<(Self, SessionInfo), SqlxError> {
        let mut this = Self(ws);
        let session_info = this.login(opts).await?;
        Ok((this, session_info))
    }

    /// Receives an uncompressed [`Response<T>`].
    pub async fn recv<T>(&mut self) -> Result<(T, Option<Attributes>), SqlxError>
    where
        T: DeserializeOwned,
    {
        let Some(bytes) = self.try_next().await? else {
            return Err(ExaProtocolError::from(None))?;
        };

        let res: Result<Response<_>, _> = serde_json::from_slice(&bytes);
        let response = res.map_err(ToSqlxError::to_sqlx_err)?;
        Result::from(response).map_err(From::from)
    }

    /// The login process consists of sending the desired login command,
    /// optionally receiving some response data from it, and then
    /// finishing the login by sending the connection options.
    ///
    /// It is ALWAYS uncompressed.
    async fn login(
        &mut self,
        mut opts: ExaConnectOptionsRef<'_>,
    ) -> Result<SessionInfo, SqlxError> {
        match &mut opts.login {
            LoginRef::Credentials(creds) => self.login_creds(creds, opts.protocol_version).await?,
            _ => self.login_token(opts.protocol_version).await?,
        }

        let cmd = (&opts).try_into()?;
        self.get_session_info(cmd).await
    }

    /// Starts the login flow using a username and password.
    async fn login_creds(
        &mut self,
        credentials: &mut CredentialsRef<'_>,
        protocol_version: ProtocolVersion,
    ) -> Result<(), SqlxError> {
        let key = self.get_public_key(protocol_version).await?;
        credentials.encrypt_password(&key)?;
        Ok(())
    }

    /// Starts the login flow using a token.
    async fn login_token(&mut self, protocol_version: ProtocolVersion) -> Result<(), SqlxError> {
        let cmd = ExaCommand::new_login_token(protocol_version).try_into()?;
        self.send(cmd).await?;
        self.recv::<Option<IgnoredAny>>().await?;
        Ok(())
    }

    async fn get_public_key(
        &mut self,
        protocol_version: ProtocolVersion,
    ) -> Result<RsaPublicKey, SqlxError> {
        let cmd = ExaCommand::new_login(protocol_version).try_into()?;
        self.send(cmd).await?;
        self.recv::<PublicKey>().await.map(|(key, _)| key.into())
    }

    async fn get_session_info(&mut self, cmd: String) -> Result<SessionInfo, SqlxError> {
        self.send(cmd).await?;
        self.recv().await.map(|(k, _)| k)
    }
}

impl Stream for PlainWebSocket {
    type Item = Result<Bytes, SqlxError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let Some(msg) = ready!(self.0.poll_next_unpin(cx)) else {
            return Poll::Ready(None);
        };

        let bytes = match msg.map_err(ToSqlxError::to_sqlx_err)? {
            Message::Text(s) => s.into(),
            Message::Binary(v) => v,
            Message::Close(c) => Err(ExaProtocolError::from(c))?,
            // Ignore other messages and pend for the next
            _ => return Poll::Pending,
        };

        Poll::Ready(Some(Ok(bytes)))
    }
}

impl Sink<String> for PlainWebSocket {
    type Error = SqlxError;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.0
            .poll_ready_unpin(cx)
            .map_err(ToSqlxError::to_sqlx_err)
    }

    fn start_send(mut self: Pin<&mut Self>, item: String) -> Result<(), Self::Error> {
        self.0
            .start_send_unpin(Message::Text(item.into()))
            .map_err(ToSqlxError::to_sqlx_err)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.0
            .poll_flush_unpin(cx)
            .map_err(ToSqlxError::to_sqlx_err)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.0
            .poll_close_unpin(cx)
            .map_err(ToSqlxError::to_sqlx_err)
    }
}
