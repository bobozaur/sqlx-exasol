mod extend;
pub mod socket;
mod tls;

#[cfg(feature = "etl")]
use std::net::IpAddr;
use std::{borrow::Cow, fmt::Debug, net::SocketAddr};

use extend::WebSocketExt;
use futures_util::{io::BufReader, Future};
use lru::LruCache;
use rsa::RsaPublicKey;
use serde::de::{DeserializeOwned, IgnoredAny};
use socket::ExaSocket;
use sqlx_core::Error as SqlxError;
pub use tls::WithMaybeTlsExaSocket;

use super::stream::QueryResultStream;
#[cfg(feature = "etl")]
use crate::responses::Hosts;
use crate::{
    command::{Command, ExaCommand},
    error::{ExaProtocolError, ExaResultExt},
    options::{CredentialsRef, ExaConnectOptionsRef, LoginRef, ProtocolVersion},
    responses::{
        DataChunk, DescribeStatement, ExaAttributes, PreparedStatement, PublicKey, QueryResult,
        Results, SessionInfo,
    },
};

#[derive(Debug)]
pub struct ExaWebSocket {
    pub ws: WebSocketExt,
    pub attributes: ExaAttributes,
    pub pending_rollback: bool,
}

impl ExaWebSocket {
    const WS_SCHEME: &str = "ws";
    const WSS_SCHEME: &str = "wss";

    pub(crate) async fn new(
        host: &str,
        port: u16,
        socket: ExaSocket,
        options: ExaConnectOptionsRef<'_>,
        with_tls: bool,
    ) -> Result<(Self, SessionInfo), SqlxError> {
        let scheme = match with_tls {
            true => Self::WSS_SCHEME,
            false => Self::WS_SCHEME,
        };

        let host = format!("{scheme}://{host}:{port}");

        let (ws, _) = async_tungstenite::client_async(host, BufReader::new(socket))
            .await
            .to_sqlx_err()?;

        let ws = WebSocketExt::new(ws);

        let mut this = Self {
            ws,
            attributes: Default::default(),
            pending_rollback: false,
        };

        this.attributes.encryption_enabled = with_tls;
        this.attributes.fetch_size = options.fetch_size;
        this.attributes.statement_cache_capacity = options.statement_cache_capacity;
        let should_compress = options.compression;

        // Login is always uncompressed
        let session_info = this.login(options).await?;

        // So we set compression afterwards, if needed.
        this.attributes.compression_enabled = should_compress;
        this.ws = this.ws.adjust_compression(should_compress);
        this.get_attributes().await?;

        Ok((this, session_info))
    }

    pub(crate) async fn login(
        &mut self,
        mut opts: ExaConnectOptionsRef<'_>,
    ) -> Result<SessionInfo, SqlxError> {
        match &mut opts.login {
            LoginRef::Credentials(creds) => {
                self.login_credentials(creds, opts.protocol_version).await?
            }
            _ => self.login_token(opts.protocol_version).await?,
        }

        let cmd = (&opts).try_into()?;
        self.get_session_info(cmd).await
    }

    /// Executes a [`Command`] and returns a [`QueryResultStream`].
    pub async fn get_result_stream<'a, C, F>(
        &'a mut self,
        cmd: Command,
        rs_handle: &mut Option<u16>,
        future_maker: C,
    ) -> Result<QueryResultStream<'_, C, F>, SqlxError>
    where
        C: Fn(&'a mut ExaWebSocket, u16, usize) -> Result<F, SqlxError>,
        F: Future<Output = Result<(DataChunk, &'a mut ExaWebSocket), SqlxError>> + 'a,
    {
        if let Some(handle) = rs_handle.take() {
            self.close_result_set(handle).await?;
        }

        let query_result = self.get_query_result(cmd).await?;
        *rs_handle = query_result.handle();

        QueryResultStream::new(self, query_result, future_maker)
    }

    pub async fn get_query_result(&mut self, cmd: Command) -> Result<QueryResult, SqlxError> {
        self.send_and_recv::<Results>(cmd).await.map(From::from)
    }

    pub async fn close_result_set(&mut self, handle: u16) -> Result<(), SqlxError> {
        let cmd = ExaCommand::new_close_result(handle).try_into()?;
        self.send_cmd_ignore_response(cmd).await?;
        Ok(())
    }

    pub async fn create_prepared(&mut self, cmd: Command) -> Result<PreparedStatement, SqlxError> {
        self.send_and_recv(cmd).await
    }

    pub async fn describe(&mut self, cmd: Command) -> Result<DescribeStatement, SqlxError> {
        self.send_and_recv(cmd).await
    }

    pub async fn close_prepared(&mut self, handle: u16) -> Result<(), SqlxError> {
        let cmd = ExaCommand::new_close_prepared(handle).try_into()?;
        self.send_cmd_ignore_response(cmd).await
    }

    pub async fn fetch_chunk(&mut self, cmd: Command) -> Result<DataChunk, SqlxError> {
        self.send_and_recv(cmd).await
    }

    pub async fn set_attributes(&mut self) -> Result<(), SqlxError> {
        let cmd = ExaCommand::new_set_attributes(&self.attributes).try_into()?;
        self.send_cmd_ignore_response(cmd).await
    }

    #[cfg(feature = "etl")]
    pub async fn get_hosts(&mut self) -> Result<Vec<IpAddr>, SqlxError> {
        let host_ip = self.socket_addr().ip();
        let _cmd = ExaCommand::new_get_hosts(host_ip).try_into()?;
        self.send_and_recv::<Hosts>(_cmd).await.map(From::from)
    }

    pub async fn get_attributes(&mut self) -> Result<(), SqlxError> {
        let cmd = ExaCommand::GetAttributes.try_into()?;
        self.send_cmd_ignore_response(cmd).await
    }

    pub async fn begin(&mut self) -> Result<(), SqlxError> {
        // Exasol does not have nested transactions.
        if self.attributes.open_transaction {
            return Err(ExaProtocolError::TransactionAlreadyOpen)?;
        }

        // The next time a query is executed, the transaction will be started.
        // We could eagerly start it as well, but that implies one more
        // round-trip to the server and back with no benefit.
        self.attributes.autocommit = false;
        self.attributes.open_transaction = true;
        Ok(())
    }

    pub async fn commit(&mut self) -> Result<(), SqlxError> {
        // It's fine to set this before executing the command
        // since we want to commit anyway.
        self.attributes.autocommit = true;

        // Just changing `autocommit` attribute implies a COMMIT,
        // but we would still have to send a command to the server
        // to update it, so we might as well be explicit.
        let cmd = ExaCommand::new_execute("COMMIT;", &self.attributes).try_into()?;
        self.send_cmd_ignore_response(cmd).await?;

        self.attributes.open_transaction = false;
        Ok(())
    }

    /// Sends a rollback to the database and awaits the response.
    pub async fn rollback(&mut self) -> Result<(), SqlxError> {
        let cmd = ExaCommand::new_execute("ROLLBACK;", &self.attributes).try_into()?;
        self.raw_send(cmd).await?;
        self.recv::<Option<IgnoredAny>>().await?;

        // We explicitly set the attribute after
        // we know the rollback was successful.
        self.attributes.autocommit = true;
        self.attributes.open_transaction = false;
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
    #[cfg(feature = "migrate")]
    pub async fn execute_batch(&mut self, sql: &str) -> Result<(), SqlxError> {
        // Trim the query end so we don't have an empty statement at the end of the array.
        let sql = sql.trim_end();
        let sql = sql.strip_suffix(';').unwrap_or(sql);

        // Do a naive yet valiant attempt at splitting the query.
        let sql_batch = sql.split(';').collect();
        let cmd = ExaCommand::new_execute_batch(sql_batch, &self.attributes).try_into()?;

        // Run batch SQL command
        match self.send_cmd_ignore_response(cmd).await {
            Ok(_) => return Ok(()),
            Err(e) => tracing::warn!(
                "failed to execute batch SQL: {e}; will attempt sequential execution"
            ),
        };

        // If we got here then batch execution failed.
        // This will pretty much be happening if  there are ';' literals in the query.
        //
        // So.. we'll do an incremental identification of queries, still splitting by ';'
        // but this time, if a statement execution fails, we concatenate it with the next string
        // up until the next ';'.
        let mut result = Ok(());
        let mut position = 0;
        let mut sql_start = 0;

        let handle_err_fn = |err: SqlxError, result: &mut Result<(), SqlxError>| {
            // Exasol doesn't seem to have a dedicated code for malformed queries.
            // There's `42000` but it does not look to only be related to syntax errors.
            //
            // So we at least check if this is a database error and continue if so.
            // Otherwise something else is wrong and we can fail early.
            let db_err = match &err {
                SqlxError::Database(_) => err,
                _ => return Err(err),
            };

            tracing::warn!("error running statement: {db_err}; perhaps it's incomplete?");

            // We wanna store the first error occurred.
            // If at some point, after further concatenations, the query
            // succeeds, then we'll set the result to `Ok`.
            if result.is_ok() {
                *result = Err(db_err);
            }

            Ok(())
        };

        while let Some(sql_end) = sql[position..].find(';') {
            // Found a separator, split the SQL.
            let sql = sql[sql_start..position + sql_end].trim();

            let cmd = ExaCommand::new_execute(sql, &self.attributes).try_into()?;
            // Next lookup will be after the just encountered separator.
            position += sql_end + 1;

            if let Err(err) = self.send_cmd_ignore_response(cmd).await {
                handle_err_fn(err, &mut result)?;
            } else {
                // Yay!!!
                sql_start = position;
                result = Ok(());
            }
        }

        // We need to run the remaining statement, if any.
        let sql = sql[sql_start..].trim();

        if !sql.is_empty() {
            let cmd = ExaCommand::new_execute(sql, &self.attributes).try_into()?;
            if let Err(err) = self.send_cmd_ignore_response(cmd).await {
                handle_err_fn(err, &mut result)?;
            } else {
                result = Ok(());
            }
        }

        result
    }

    pub fn socket_addr(&self) -> SocketAddr {
        self.ws.socket_addr()
    }

    async fn login_credentials(
        &mut self,
        credentials: &mut CredentialsRef<'_>,
        protocol_version: ProtocolVersion,
    ) -> Result<(), SqlxError> {
        let key = self.get_public_key(protocol_version).await?;
        credentials.encrypt_password(key)?;
        Ok(())
    }

    async fn login_token(&mut self, protocol_version: ProtocolVersion) -> Result<(), SqlxError> {
        let cmd = ExaCommand::new_login_token(protocol_version).try_into()?;
        self.send_cmd_ignore_response(cmd).await
    }

    async fn get_public_key(
        &mut self,
        protocol_version: ProtocolVersion,
    ) -> Result<RsaPublicKey, SqlxError> {
        let cmd = ExaCommand::new_login(protocol_version).try_into()?;
        self.send_and_recv::<PublicKey>(cmd).await.map(From::from)
    }

    async fn get_session_info(&mut self, cmd: Command) -> Result<SessionInfo, SqlxError> {
        self.send_and_recv(cmd).await
    }

    /// Utility method for when the `response_data` field of the [`Response`] is not of any
    /// interest. Note that attributes will still get updated.
    async fn send_cmd_ignore_response(&mut self, cmd: Command) -> Result<(), SqlxError> {
        self.send_and_recv::<Option<IgnoredAny>>(cmd).await?;
        Ok(())
    }

    /// Sends a [`Command`] to the database and returns the `response_data` field of the
    /// [`Response`]. We will also await on the response from a previously started command, if
    /// needed.
    async fn send_and_recv<T>(&mut self, cmd: Command) -> Result<T, SqlxError>
    where
        T: DeserializeOwned + Debug,
    {
        self.send(cmd).await?;
        self.recv().await
    }

    /// Sends a [`Command`] to the database, also ensuring a rollback is issued if pending.
    pub(crate) async fn send(&mut self, cmd: Command) -> Result<(), SqlxError> {
        // If a rollback is pending, which really only happens when a transaction is dropped
        // then we need to send that first before the actual command.
        if self.pending_rollback {
            self.rollback().await?;
            self.pending_rollback = false;
        }

        self.raw_send(cmd).await
    }

    /// Sends a [`Command`] to the database.
    async fn raw_send(&mut self, cmd: Command) -> Result<(), SqlxError> {
        let cmd = cmd.into_inner();
        tracing::debug!("sending command to database: {cmd}");

        self.ws.send(cmd).await
    }

    /// Receives a database response containing attributes,
    /// processes the attributes and returns the inner data.
    pub(crate) async fn recv<T>(&mut self) -> Result<T, SqlxError>
    where
        T: DeserializeOwned + Debug,
    {
        let (response_data, attributes) = Result::from(self.ws.recv().await?)?;

        if let Some(attributes) = attributes {
            tracing::debug!("updating connection attributes using:\n{attributes:#?}");
            self.attributes.update(attributes)
        }

        tracing::trace!("database response:\n{response_data:#?}");

        Ok(response_data)
    }
}
