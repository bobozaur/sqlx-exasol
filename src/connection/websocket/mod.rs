mod transport;
pub mod socket;
mod tls;

#[cfg(feature = "etl")]
use std::net::IpAddr;
use std::{borrow::Cow, fmt::Debug, net::SocketAddr};

use transport::WebSocket;
use futures_util::{io::BufReader, Future};
use lru::LruCache;
use serde::de::{DeserializeOwned, IgnoredAny};
use socket::ExaSocket;
use sqlx_core::Error as SqlxError;
pub use tls::WithMaybeTlsExaSocket;

use self::transport::PlainWebSocket;
use super::stream::QueryResultStream;
#[cfg(feature = "etl")]
use crate::responses::Hosts;
use crate::{
    command::ExaCommand,
    error::{ExaProtocolError, ToSqlxError},
    options::ExaConnectOptionsRef,
    responses::{
        DataChunk, DescribeStatement, ExaAttributes, PreparedStatement, QueryResult, Results,
        SessionInfo,
    },
};

/// A websocket connected to the Exasol, providing lower level
/// interaction with the database.
#[derive(Debug)]
pub struct ExaWebSocket {
    pub ws: WebSocket,
    pub attributes: ExaAttributes,
    pub pending_rollback: bool,
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

        // Login is always uncompressed
        let mut plain_ws = PlainWebSocket(ws);
        let session_info = plain_ws.login(options).await?;
        let ws = WebSocket::new(plain_ws.0, attributes.compression_enabled());

        let mut this = Self {
            ws,
            attributes,
            pending_rollback: false,
        };

        this.get_attributes().await?;

        Ok((this, session_info))
    }

    /// Executes a command and returns a [`QueryResultStream`].
    pub async fn get_result_stream<'a, C, F>(
        &'a mut self,
        cmd: String,
        rs_handle: &mut Option<u16>,
        future_maker: C,
    ) -> Result<QueryResultStream<'a, C, F>, SqlxError>
    where
        C: Fn(&'a mut ExaWebSocket, u16, usize) -> Result<F, SqlxError>,
        F: Future<Output = Result<(DataChunk, &'a mut ExaWebSocket), SqlxError>> + 'a,
    {
        // Close the previously opened result set, if any.
        if let Some(handle) = rs_handle.take() {
            self.close_result_set(handle).await?;
        }

        let query_result = self.get_query_result(cmd).await?;
        *rs_handle = query_result.handle();

        QueryResultStream::new(self, query_result, future_maker)
    }

    pub async fn get_query_result(&mut self, cmd: String) -> Result<QueryResult, SqlxError> {
        self.transfer::<Results>(cmd).await.map(From::from)
    }

    pub async fn close_result_set(&mut self, handle: u16) -> Result<(), SqlxError> {
        let cmd = ExaCommand::new_close_result(handle).try_into()?;
        self.send_cmd_ignore_response(cmd).await?;
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
        self.send_cmd_ignore_response(cmd).await
    }

    pub async fn fetch_chunk(&mut self, cmd: String) -> Result<DataChunk, SqlxError> {
        self.transfer(cmd).await
    }

    pub async fn set_attributes(&mut self) -> Result<(), SqlxError> {
        let cmd = ExaCommand::new_set_attributes(&self.attributes).try_into()?;
        self.send_cmd_ignore_response(cmd).await
    }

    #[cfg(feature = "etl")]
    pub async fn get_hosts(&mut self) -> Result<Vec<IpAddr>, SqlxError> {
        let host_ip = self.socket_addr().ip();
        let cmd = ExaCommand::new_get_hosts(host_ip).try_into()?;
        self.transfer::<Hosts>(cmd).await.map(From::from)
    }

    pub async fn get_attributes(&mut self) -> Result<(), SqlxError> {
        let cmd = ExaCommand::GetAttributes.try_into()?;
        self.send_cmd_ignore_response(cmd).await
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
        // It's fine to set this before executing the command
        // since we want to commit anyway.
        self.attributes.set_autocommit(true);

        // Just changing `autocommit` attribute implies a COMMIT,
        // but we would still have to send a command to the server
        // to update it, so we might as well be explicit.
        let cmd = ExaCommand::new_execute("COMMIT;", &self.attributes).try_into()?;
        self.send_cmd_ignore_response(cmd).await?;

        self.attributes.set_open_transaction(false);
        Ok(())
    }

    /// Sends a rollback to the database and awaits the response.
    pub async fn rollback(&mut self) -> Result<(), SqlxError> {
        let cmd = ExaCommand::new_execute("ROLLBACK;", &self.attributes).try_into()?;
        self.raw_send(cmd).await?;
        self.recv::<Option<IgnoredAny>>().await?;

        // We explicitly set the attribute after
        // we know the rollback was successful.
        self.attributes.set_autocommit(true);
        self.attributes.set_open_transaction(false);
        self.pending_rollback = false;
        Ok(())
    }

    pub async fn ping(&mut self) -> Result<(), SqlxError> {
        self.ws.ping().await
    }

    pub async fn disconnect(&mut self) -> Result<(), SqlxError> {
        let cmd = ExaCommand::Disconnect.try_into()?;
        self.send_cmd_ignore_response(cmd).await
    }

    pub async fn close(&mut self) -> Result<(), SqlxError> {
        self.ws.close().await
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

        // Run batch SQL command
        self.send_cmd_ignore_response(cmd).await
    }

    pub fn socket_addr(&self) -> SocketAddr {
        self.ws.socket_addr()
    }

    /// Utility method for when the `response_data` field of the [`Response`] is not of any
    /// interest. Note that attributes will still get updated.
    async fn send_cmd_ignore_response(&mut self, cmd: String) -> Result<(), SqlxError> {
        self.transfer::<Option<IgnoredAny>>(cmd).await?;
        Ok(())
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

    /// Sends a command to the database, also ensuring a rollback is issued if pending.
    pub async fn send(&mut self, cmd: String) -> Result<(), SqlxError> {
        // If a rollback is pending, which really only happens when a transaction is dropped
        // then we need to send that first before the actual command.
        if self.pending_rollback {
            self.rollback().await?;
            self.pending_rollback = false;
        }

        self.raw_send(cmd).await
    }

    /// Sends a command to the database.
    async fn raw_send(&mut self, cmd: String) -> Result<(), SqlxError> {
        tracing::debug!("sending command to database: {cmd}");
        self.ws.send(cmd).await
    }

    /// Receives a database response containing attributes,
    /// processes the attributes and returns the inner data.
    pub async fn recv<T>(&mut self) -> Result<T, SqlxError>
    where
        T: DeserializeOwned + Debug,
    {
        let (response_data, attributes) = Result::from(self.ws.recv().await?)?;

        if let Some(attributes) = attributes {
            tracing::debug!("updating connection attributes using:\n{attributes:#?}");
            self.attributes.update(attributes);
        }

        tracing::trace!("database response:\n{response_data:#?}");

        Ok(response_data)
    }
}
