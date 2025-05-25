use async_tungstenite::{tungstenite::Message, WebSocketStream};
use futures_util::{io::BufReader, StreamExt};
use rsa::RsaPublicKey;
use serde::de::{DeserializeOwned, IgnoredAny};
use sqlx_core::Error as SqlxError;

use crate::{
    command::ExaCommand,
    connection::websocket::socket::ExaSocket,
    error::{ExaProtocolError, ToSqlxError},
    options::{CredentialsRef, ExaConnectOptionsRef, LoginRef},
    responses::{PublicKey, Response, SessionInfo},
    ProtocolVersion,
};

#[derive(Debug)]
pub struct PlainWebSocket(pub WebSocketStream<BufReader<ExaSocket>>);

impl PlainWebSocket {
    /// The login process consists of sending the desired login command,
    /// optionally receiving some response data from it, and then
    /// finishing the login by sending the connection options.
    ///
    /// It is ALWAYS uncompressed.
    pub async fn login(
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
        self.recv_data::<Option<IgnoredAny>>().await?;
        Ok(())
    }

    async fn get_public_key(
        &mut self,
        protocol_version: ProtocolVersion,
    ) -> Result<RsaPublicKey, SqlxError> {
        let cmd = ExaCommand::new_login(protocol_version).try_into()?;
        self.send(cmd).await?;
        self.recv_data::<PublicKey>().await.map(From::from)
    }

    async fn get_session_info(&mut self, cmd: String) -> Result<SessionInfo, SqlxError> {
        self.send(cmd).await?;
        self.recv_data().await
    }

    /// Sends an uncompressed command.
    pub async fn send(&mut self, cmd: String) -> Result<(), SqlxError> {
        self.0
            .send(Message::Text(cmd.into()))
            .await
            .map_err(ToSqlxError::to_sqlx_err)
    }

    /// Receives an uncompressed [`Response<T>`].
    pub async fn recv<T>(&mut self) -> Result<Response<T>, SqlxError>
    where
        T: DeserializeOwned,
    {
        while let Some(response) = self.0.next().await {
            let msg = response.map_err(ToSqlxError::to_sqlx_err)?;

            return match msg {
                Message::Text(s) => serde_json::from_str(&s).map_err(ToSqlxError::to_sqlx_err),
                Message::Binary(v) => serde_json::from_slice(&v).map_err(ToSqlxError::to_sqlx_err),
                Message::Close(c) => {
                    self.close().await.ok();
                    Err(ExaProtocolError::from(c))?
                }
                _ => continue,
            };
        }

        Err(ExaProtocolError::MissingMessage)?
    }

    pub async fn close(&mut self) -> Result<(), SqlxError> {
        self.0.close(None).await.map_err(ToSqlxError::to_sqlx_err)?;
        Ok(())
    }

    /// Awaits the receipt of a response from the database.
    /// Since this is only used throughout the login process, we ignore
    /// returned attributes, if any.
    ///
    /// We will anyway issue a `getAttributes` command after the login is done.
    async fn recv_data<T>(&mut self) -> Result<T, SqlxError>
    where
        T: DeserializeOwned,
    {
        let (response_data, _) = Result::from(self.recv().await?)?;
        Ok(response_data)
    }
}
