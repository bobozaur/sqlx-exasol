pub mod maybe_tls;
mod socket_addr;

use std::{
    future::Future,
    io,
    net::{IpAddr, SocketAddr, SocketAddrV4},
    sync::{atomic::AtomicBool, Arc},
};

use flume::{Receiver, Sender};
use futures_core::future::BoxFuture;
use futures_util::task::AtomicWaker;
use sqlx_core::{
    net::WithSocket,
    rt::JoinHandle,
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
        server::ServerTask,
        EtlQuery,
    },
    ExaConnection, SqlxResult,
};

/// Alias for a future that sets up a socket to be used by an HTTP server.
///
/// This future will perform the necessary TCP connection and TLS handshake (if needed).
/// The resulting [`ExaSocket`] is then used by the one-shot HTTP server for an ETL job.
///
/// This is necessary because Exasol initiates the TLS handshake for each socket sequentially.
pub type SocketHandshake = BoxFuture<'static, io::Result<ExaSocket>>;

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

    /// The type of the channel half used to send data between the HTTP server and the worker.
    type DataPipe: Send;

    /// Whether to use compression for the ETL job.
    /// If `None`, the connection's setting is used.
    fn use_compression(&self) -> Option<bool>;

    /// The number of workers to create for this job.
    /// If 0, one worker per Exasol node will be created.
    fn num_workers(&self) -> usize;

    /// Creates a new worker instance.
    ///
    /// The worker will receive the data pipe from the HTTP server through the provided `rx`
    /// channel once the HTTP request is received.
    fn create_worker(&self, rx: Receiver<Self::DataPipe>, with_compression: bool) -> Self::Worker;

    /// Creates and spawns a one-shot HTTP server task.
    ///
    /// This task will handle one HTTP request from Exasol and transfer the data pipe to the
    /// worker via the provided `tx` channel once the HTTP request is received.
    fn create_server_task(
        &self,
        tx: Sender<Self::DataPipe>,
        socket_future: SocketHandshake,
        waker: Arc<AtomicWaker>,
        stop: Arc<AtomicBool>,
    ) -> JoinHandle<io::Result<()>>;

    /// Connects a socket for each Exasol node, spawns a one-shot HTTP server task for each, and
    /// creates the ETL workers.
    fn connect(
        &self,
        wsm: WithMaybeTlsSocketMaker,
        ips: Vec<IpAddr>,
        port: u16,
        with_compression: bool,
        stop_tasks: Arc<AtomicBool>,
    ) -> impl Future<Output = SqlxResult<JobComponents<Self::Worker>>> + Send {
        async move {
            let num = self.num_workers();
            let num = if num > 0 { num } else { ips.len() };

            let (tx, rx) = flume::bounded(0);
            let mut addrs = Vec::with_capacity(num);
            let mut workers = Vec::with_capacity(num);
            let mut server_tasks = Vec::with_capacity(num);

            for ip in ips.into_iter().take(num) {
                let with_exa_socket = WithExaSocket(SocketAddr::new(ip, port));
                let with_socket = WithSocketAddr(wsm.make_with_socket(with_exa_socket));

                let (addr, socket_fut) =
                    sqlx_core::net::connect_tcp(&ip.to_string(), port, with_socket).await??;

                let waker = Arc::new(AtomicWaker::new());
                let worker = self.create_worker(rx.clone(), with_compression);
                let handle = self.create_server_task(
                    tx.clone(),
                    socket_fut,
                    waker.clone(),
                    stop_tasks.clone(),
                );

                addrs.push(addr);
                workers.push(worker);
                server_tasks.push(ServerTask::new(handle, waker));
            }

            Ok(JobComponents {
                addrs,
                workers,
                server_tasks,
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
            let stop_tasks = Arc::new(AtomicBool::new(false));

            // Get the internal Exasol node addresses and the workers
            let JobComponents {
                addrs,
                workers,
                server_tasks,
            } = self
                .connect(wsm, ips, port, with_compression, stop_tasks.clone())
                .await?;

            // Query execution driving future to be returned and awaited alongside the worker IO
            let query = AssertSqlSafe(self.query(addrs, with_tls, with_compression)).into_sql_str();
            let query_future = ExecuteEtl(ExaRoundtrip::new(Execute(query))).future(&mut conn.ws);

            Ok((
                EtlQuery::new(query_future, server_tasks, stop_tasks),
                workers,
            ))
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
    /// The spawned HTTP server tasks.
    server_tasks: Vec<ServerTask>,
}

/// A trait for creating a [`WithSocket`] instance that produces a [`SocketHandshake`].
///
/// This trait is mainly used to allow sharing the TLS config between sockets by creating
/// multiple [`WithSocket`] instances.
pub trait WithSocketMaker: Send + Sync {
    type WithSocket: WithSocket<Output = SocketHandshake> + Send;

    fn make_with_socket(&self, with_socket: WithExaSocket) -> Self::WithSocket;
}
