pub mod maybe_tls;
mod socket_addr;

use std::{
    future::Future,
    io,
    net::{IpAddr, SocketAddr, SocketAddrV4},
};

use flume::{Receiver, Sender};
use futures_core::future::BoxFuture;
use hyper::server::conn::http1::Connection;
use sqlx_core::{
    net::WithSocket,
    sql_str::{AssertSqlSafe, SqlSafeStr},
};

use crate::{
    connection::websocket::{
        future::{ExaRoundtrip, GetHosts, WebSocketFuture},
        request::Execute,
        socket::{ExaSocket, WithExaSocket},
    },
    etl::{
        job::{maybe_tls::WithMaybeTlsSocketMaker, socket_addr::WithSocketAddr},
        query::ExecuteEtl,
        server::{OneShotService, WithHttpServer},
        EtlQuery,
    },
    ExaConnection, SqlxResult,
};

/// Alias for futures that perform the necessary TCP connection, TLS handshake (if needed),
/// set up a one-shot HTTP Server, and handle the data channel and connection handover to the IO
/// worker.
pub type ServerBootstrap = BoxFuture<'static, io::Result<()>>;

/// Alias for a `hyper` one-shot HTTP connection.
///
/// The IO worker will poll this connection to completion.
pub type OneShotServer<S> = Connection<ExaSocket, S>;

/// Interface for ETL jobs, containing common functionality required by both `IMPORT` and `EXPORT`
/// operations.
///
/// This trait is implemented by [`super::ImportBuilder`] and [`super::ExportBuilder`] and provides
/// the logic for setting up the ETL job.
pub trait EtlJob: Sized + Send + Sync {
    const DEFAULT_BUF_SIZE: usize = 65536;

    const GZ_FILE_EXT: &'static str = "gz";
    const CSV_FILE_EXT: &'static str = "csv";

    const HTTP_SCHEME: &'static str = "http";
    const HTTPS_SCHEME: &'static str = "https";

    const JOB_TYPE: &'static str;

    /// The type of worker that will be created for this job.
    /// Either [`super::ExaImport`] or [`super::ExaExport`].
    type Worker: Send;

    /// The HTTP service implementation.
    type Service: OneShotService;

    /// The type of the channel half used to send data between the HTTP server and the worker.
    type DataPipe: Send + 'static;

    /// Whether to use compression for the ETL job.
    /// If `None`, the connection's setting is used.
    fn use_compression(&self) -> Option<bool>;

    /// The number of workers to create for this job.
    /// If 0, one worker per Exasol node will be created.
    fn num_workers(&self) -> usize;

    /// Creates a new worker instance.
    ///
    /// The worker receives the data pipe and the HTTP connection from the server through the
    /// provided `parts_rx` channel.
    ///
    /// The worker is responsible for polling the HTTP connection to completion after
    /// the query future bootstraps it.
    fn create_worker(
        &self,
        parts_rx: Receiver<(Self::DataPipe, OneShotServer<Self::Service>)>,
        with_compression: bool,
    ) -> Self::Worker;

    /// Creates the HTTP service that will be used by the one-shot HTTP server.
    ///
    /// The service will send the data pipe to the worker through the provided `chan_tx` channel.
    fn create_service(&self, chan_tx: Sender<Self::DataPipe>) -> Self::Service;

    /// For each Exasol node, this method creates an ETL worker and a corresponding server bootstrap
    /// future.
    ///
    /// The bootstrap future will connect a socket, perform the TLS handshake (if needed),
    /// set up the HTTP server, and handle the handover of the data pipe and HTTP connection to the
    /// worker.
    fn connect(
        &self,
        wsm: WithMaybeTlsSocketMaker,
        ips: Vec<IpAddr>,
        port: u16,
        with_compression: bool,
    ) -> impl Future<Output = SqlxResult<JobComponents<Self::Worker>>> + Send {
        async move {
            let num = self.num_workers();
            let num = if num > 0 { num } else { ips.len() };

            let (parts_tx, parts_rx) = flume::bounded(0);
            let (chan_tx, chan_rx) = flume::bounded(0);
            let mut addrs = Vec::with_capacity(num);
            let mut workers = Vec::with_capacity(num);
            let mut conn_futures = Vec::with_capacity(num);

            for ip in ips.into_iter().take(num) {
                let service: <Self as EtlJob>::Service = self.create_service(chan_tx.clone());

                let with_exa_socket = WithExaSocket(SocketAddr::new(ip, port));
                let with_maybe_tls_socket = wsm.make_with_socket(with_exa_socket);
                let with_http_server = WithHttpServer::new(
                    with_maybe_tls_socket,
                    service,
                    chan_rx.clone(),
                    parts_tx.clone(),
                );
                let with_socket = WithSocketAddr(with_http_server);

                let (addr, conn_future) =
                    sqlx_core::net::connect_tcp(&ip.to_string(), port, with_socket).await??;

                let worker = self.create_worker(parts_rx.clone(), with_compression);

                addrs.push(addr);
                workers.push(worker);
                conn_futures.push(conn_future);
            }

            Ok(JobComponents {
                addrs,
                workers,
                conn_futures,
            })
        }
    }

    fn query(&self, addrs: Vec<SocketAddrV4>, with_tls: bool, with_compression: bool) -> String;

    /// Builds an ETL job, returning the query future and the ETL workers.
    ///
    /// The returned [`EtlQuery`] future must be awaited to drive the ETL job to completion.
    fn build_job<'a, 'c>(
        &'a self,
        conn: &'c mut ExaConnection,
    ) -> impl Future<Output = SqlxResult<(EtlQuery<'c>, Vec<Self::Worker>)>> + Send
    where
        'c: 'a,
    {
        async {
            let socket_addr = conn.server();
            let port = socket_addr.port();

            let ips = GetHosts::new(socket_addr.ip())
                .future(&mut conn.ws)
                .await?
                .into();

            let with_tls = conn.attributes().encryption_enabled();
            let with_compression = self
                .use_compression()
                .unwrap_or(conn.attributes().compression_enabled());
            let wsm = WithMaybeTlsSocketMaker::new(with_tls)?;

            // Get the internal Exasol node addresses and the workers
            let JobComponents {
                addrs,
                workers,
                conn_futures,
            } = self.connect(wsm, ips, port, with_compression).await?;

            // Query execution driving future to be returned and awaited alongside the worker IO
            let query = AssertSqlSafe(self.query(addrs, with_tls, with_compression)).into_sql_str();
            let query_future = ExecuteEtl(ExaRoundtrip::new(Execute(query))).future(&mut conn.ws);

            Ok((EtlQuery::new(query_future, conn_futures), workers))
        }
    }

    /// Generates and appends the internal files Exasol will read/write from/to
    /// and adds them to the ETL query.
    fn append_files(
        query: &mut String,
        addrs: Vec<SocketAddrV4>,
        with_tls: bool,
        with_compression: bool,
    ) {
        let prefix = if with_tls {
            Self::HTTPS_SCHEME
        } else {
            Self::HTTP_SCHEME
        };

        let ext = if with_compression {
            Self::GZ_FILE_EXT
        } else {
            Self::CSV_FILE_EXT
        };

        for (idx, addr) in addrs.into_iter().enumerate() {
            let filename = format!(
                "AT '{}://{}' FILE '{}_{:0>5}.{}'\n",
                prefix,
                addr,
                Self::JOB_TYPE,
                idx,
                ext
            );
            query.push_str(&filename);
        }
    }

    fn push_comment(query: &mut String, comment: &str) {
        query.push_str("/*\n");
        query.push_str(comment);
        query.push_str("*/\n");
    }

    fn push_ident(query: &mut String, ident: &str) {
        query.push('"');
        query.push_str(ident);
        query.push('"');
    }

    fn push_literal(query: &mut String, lit: &str) {
        query.push('\'');
        query.push_str(lit);
        query.push_str("' ");
    }

    fn push_key_value(query: &mut String, key: &str, value: &str) {
        query.push_str(key);
        query.push_str(" = ");
        Self::push_literal(query, value);
    }
}

/// A struct holding the components of an ETL job after connection setup.
struct JobComponents<W> {
    /// The internal socket addresses of the Exasol nodes.
    addrs: Vec<SocketAddrV4>,
    /// The ETL workers.
    workers: Vec<W>,
    /// The HTTP server bootstrap futures.
    conn_futures: Vec<ServerBootstrap>,
}

/// A trait for creating multiple [`WithSocket`] instances.
///
/// This trait is mainly used to allow sharing the TLS config between sockets by creating
/// multiple [`WithSocket`] instances.
pub trait WithSocketMaker: Send + Sync {
    type WithSocket: WithSocket<Output = io::Result<ExaSocket>> + Send;

    fn make_with_socket(&self, with_socket: WithExaSocket) -> Self::WithSocket;
}
