pub mod futures;
pub mod socket;
mod tls;
mod transport;

#[cfg(feature = "etl")]
use std::net::IpAddr;
use std::{
    borrow::Cow,
    fmt::Debug,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;
use futures_core::Stream;
use futures_util::{io::BufReader, Sink, SinkExt, StreamExt};
use lru::LruCache;
use serde::de::{DeserializeOwned, IgnoredAny};
use socket::ExaSocket;
use sqlx_core::Error as SqlxError;
pub use tls::WithMaybeTlsExaSocket;
use transport::MaybeCompressedWebSocket;

#[cfg(feature = "etl")]
use crate::responses::Hosts;
use crate::{
    command::ExaCommand,
    connection::stream::QueryResultStream,
    error::{ExaProtocolError, ToSqlxError},
    options::ExaConnectOptionsRef,
    responses::{
        DataChunk, DescribeStatement, ExaAttributes, PreparedStatement, QueryResult, SessionInfo,
        SingleResult,
    },
};

/// A websocket connected to the Exasol, providing lower level
/// interaction with the database.
#[derive(Debug)]
pub struct ExaWebSocket {
    pub ws: MaybeCompressedWebSocket,
    pub attributes: ExaAttributes,
    pub last_rs_handle: Option<u16>,
}

impl ExaWebSocket {
    const WS_SCHEME: &'static str = "ws";
    const WSS_SCHEME: &'static str = "wss";

    pub async fn new(
        host: &str,
        port: u16,
        socket: ExaSocket,
        options: ExaConnectOptionsRef<'_>,
        with_tls: bool,
    ) -> Result<(Self, SessionInfo), SqlxError> {
        let scheme = if with_tls {
            Self::WSS_SCHEME
        } else {
            Self::WS_SCHEME
        };

        let host = format!("{scheme}://{host}:{port}");

        let (ws, _) = async_tungstenite::client_async(host, BufReader::new(socket))
            .await
            .map_err(ToSqlxError::to_sqlx_err)?;

        let attributes = ExaAttributes::new(
            options.compression,
            options.fetch_size,
            with_tls,
            options.statement_cache_capacity,
        );

        let (ws, session_info) =
            MaybeCompressedWebSocket::new(ws, attributes.compression_enabled(), options).await?;

        let mut this = Self {
            ws,
            attributes,
            last_rs_handle: None,
        };
        this.get_attributes().await?;

        Ok((this, session_info))
    }

    /// Executes a command and returns a [`QueryResultStream`].
    pub async fn get_result_stream<'a>(
        &'a mut self,
        cmd: String,
        rs_handle: &mut Option<u16>,
    ) -> Result<QueryResultStream<'a>, SqlxError> {
        // Close the previously opened result set, if any.
        if let Some(handle) = rs_handle.take() {
            self.close_result_set(handle).await?;
        }

        let query_result = self.get_query_result(cmd).await?;
        *rs_handle = query_result.handle();

        QueryResultStream::new(self, query_result)
    }

    pub async fn get_query_result(&mut self, cmd: String) -> Result<QueryResult, SqlxError> {
        self.transfer::<SingleResult>(cmd).await.map(From::from)
    }

    pub async fn close_result_set(&mut self, handle: u16) -> Result<(), SqlxError> {
        let cmd = ExaCommand::new_close_result(handle).try_into()?;
        self.transfer::<Option<IgnoredAny>>(cmd).await?;
        Ok(())
    }

    pub async fn create_prepared(&mut self, cmd: String) -> Result<PreparedStatement, SqlxError> {
        self.transfer(cmd).await
    }

    pub async fn describe(&mut self, cmd: String) -> Result<DescribeStatement, SqlxError> {
        self.transfer(cmd).await
    }

    pub async fn close_prepared(&mut self, handle: u16) -> Result<(), SqlxError> {
        let cmd = ExaCommand::new_close_prepared(handle).try_into()?;
        self.transfer::<Option<IgnoredAny>>(cmd).await?;
        Ok(())
    }

    pub async fn fetch_chunk(&mut self, cmd: String) -> Result<DataChunk, SqlxError> {
        self.transfer(cmd).await
    }

    pub async fn set_attributes(&mut self) -> Result<(), SqlxError> {
        let cmd = ExaCommand::new_set_attributes(&self.attributes).try_into()?;
        self.transfer::<Option<IgnoredAny>>(cmd).await?;
        Ok(())
    }

    #[cfg(feature = "etl")]
    pub async fn get_hosts(&mut self) -> Result<Vec<IpAddr>, SqlxError> {
        let host_ip = self.socket_addr().ip();
        let cmd = ExaCommand::new_get_hosts(host_ip).try_into()?;
        self.transfer::<Hosts>(cmd).await.map(From::from)
    }

    pub async fn get_attributes(&mut self) -> Result<(), SqlxError> {
        let cmd = ExaCommand::GetAttributes.try_into()?;
        self.transfer::<Option<IgnoredAny>>(cmd).await?;
        Ok(())
    }

    pub fn begin(&mut self) -> Result<(), SqlxError> {
        // Exasol does not have nested transactions.
        if self.attributes.open_transaction() {
            return Err(ExaProtocolError::TransactionAlreadyOpen)?;
        }

        // The next time a query is executed, the transaction will be started.
        // We could eagerly start it as well, but that implies one more
        // round-trip to the server and back with no benefit.
        self.attributes.set_autocommit(false);
        self.attributes.set_open_transaction(true);
        Ok(())
    }

    pub async fn commit(&mut self) -> Result<(), SqlxError> {
        self.attributes.set_autocommit(true);
        self.attributes.set_open_transaction(false);

        // Just changing `autocommit` attribute implies a COMMIT,
        // but we would still have to send a command to the server
        // to update it, so we might as well be explicit.
        let cmd = ExaCommand::new_execute("COMMIT;", &self.attributes).try_into()?;
        self.transfer::<Option<IgnoredAny>>(cmd).await?;

        Ok(())
    }

    /// Sends a rollback to the database and awaits the response.
    pub async fn rollback(&mut self) -> Result<(), SqlxError> {
        let cmd = ExaCommand::new_execute("ROLLBACK;", &self.attributes).try_into()?;
        self.send(cmd).await?;
        self.recv::<Option<IgnoredAny>>().await?;

        // We explicitly set the attribute after
        // we know the rollback was successful.
        self.attributes.set_autocommit(true);
        self.attributes.set_open_transaction(false);
        Ok(())
    }

    pub async fn ping(&mut self) -> Result<(), SqlxError> {
        self.ws.ping().await
    }

    pub async fn disconnect(&mut self) -> Result<(), SqlxError> {
        let cmd = ExaCommand::Disconnect.try_into()?;
        self.transfer::<Option<IgnoredAny>>(cmd).await?;
        Ok(())
    }

    /// Returns the metadata related to a [`PreparedStatement`].
    /// If the metadata is not yet cached, a request will be first
    /// made to the database to prepare the statement and cache its metadata.
    pub async fn get_or_prepare<'a>(
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
            if let Some((_, old)) = cache.push(sql.to_owned(), prepared) {
                self.close_prepared(old.statement_handle).await?;
            }

            return Ok(Cow::Borrowed(cache.get(sql).unwrap()));
        }

        Ok(Cow::Owned(prepared))
    }

    /// Tries to take advance of the batch SQL execution command provided by Exasol.
    /// The command however implies splitting the SQL into an array of statements.
    ///
    /// Since this is finicky, we'll only use it for migration purposes, where
    /// batch SQL is actually expected/encouraged.
    ///
    /// If batch execution fails, we will try to separate statements and execute them
    /// one by one.
    pub async fn execute_batch(&mut self, sql_texts: &[&str]) -> Result<(), SqlxError> {
        let cmd = ExaCommand::new_execute_batch(sql_texts, &self.attributes).try_into()?;
        self.transfer::<Option<IgnoredAny>>(cmd).await?;
        Ok(())
    }

    /// Receives a database response containing attributes,
    /// processes the attributes and returns the inner data.
    pub async fn recv<T>(&mut self) -> Result<T, SqlxError>
    where
        T: DeserializeOwned + Debug,
    {
        let (response_data, attributes) = self.ws.recv().await?;

        if let Some(attributes) = attributes {
            tracing::debug!("updating connection attributes using:\n{attributes:#?}");
            self.attributes.update(attributes);
        }

        tracing::trace!("database response:\n{response_data:#?}");

        Ok(response_data)
    }

    pub fn socket_addr(&self) -> SocketAddr {
        self.ws.socket_addr()
    }

    /// Sends a command to the database and returns the `response_data` field of the
    /// [`Response`]. We will also await on the response from a previously started command, if
    /// needed.
    async fn transfer<T>(&mut self, cmd: String) -> Result<T, SqlxError>
    where
        T: DeserializeOwned + Debug,
    {
        self.send(cmd).await?;
        self.recv().await
    }
}

impl Stream for ExaWebSocket {
    type Item = Result<Bytes, SqlxError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().ws.poll_next_unpin(cx)
    }
}

impl Sink<String> for ExaWebSocket {
    type Error = SqlxError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut().poll_ready_unpin(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: String) -> Result<(), Self::Error> {
        self.get_mut().start_send_unpin(item)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut().poll_flush_unpin(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut().poll_close_unpin(cx)
    }
}
