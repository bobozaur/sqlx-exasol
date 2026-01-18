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
/// Typically means doing the TLS handshake.
///
/// Still used in non-TLS contexts as it makes matters easier to reason about.
/// In non-TLS cases, it ends up being an instantly ready future, since there's no handshake.
///
/// Needed because Exasol will only perform handshakes as it makes HTTP requests, which
/// will happen sequentially in IMPORT jobs.
pub type SocketHandshake = BoxFuture<'static, io::Result<ExaSocket>>;

/// Interface for ETL jobs, containing common functionality required by both IMPORT/EXPORT
/// operations.
pub trait EtlJob: Sized + Send + Sync {
    const DEFAULT_BUF_SIZE: usize = 65536;

    const GZ_FILE_EXT: &'static str = "gz";
    const CSV_FILE_EXT: &'static str = "csv";

    const HTTP_SCHEME: &'static str = "http";
    const HTTPS_SCHEME: &'static str = "https";

    const JOB_TYPE: &'static str;

    type Worker: Send;
    type DataSender: Send;

    fn use_compression(&self) -> Option<bool>;

    fn num_workers(&self) -> usize;

    fn create_worker(&self, rx: Receiver<Self::DataSender>, with_compression: bool)
        -> Self::Worker;

    fn create_server_task(
        &self,
        tx: Sender<Self::DataSender>,
        socket_future: SocketHandshake,
        waker: Arc<AtomicWaker>,
        stop: Arc<AtomicBool>,
    ) -> JoinHandle<io::Result<()>>;

    /// Connects a socket for each Exasol node.
    ///
    /// Returns three [`Vec`]:
    /// - internal socket addresses (used in query generation)
    /// - ETL workers (exposed to consumers)
    /// - handles for the spawned HTTP server tasks (for task handling)
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
                server_tasks.push(ServerTask { handle, waker });
            }

            Ok(JobComponents {
                addrs,
                workers,
                server_tasks,
            })
        }
    }

    fn query(&self, addrs: Vec<SocketAddrV4>, with_tls: bool, with_compression: bool) -> String;

    /// Builds an ETL job comprising of a [`EtlQuery`], that drives the execution of the ETL query,
    /// and an array of workers that perform the IO.
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
    /// and adds them to the provided query.
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

struct JobComponents<W> {
    addrs: Vec<SocketAddrV4>,
    workers: Vec<W>,
    server_tasks: Vec<ServerTask>,
}

/// Trait used as an interface for constructing a type implementing [`WithSocket`] that outputs a
/// future which sets up a socket to be used by an ETL worker.
///
/// One purpose of this trait is to abstract away the TLS/non-TLS workers. In TLS scenarios, we
/// might be connecting multiple sockets with the same TLS config.
pub trait WithSocketMaker: Send + Sync {
    type WithSocket: WithSocket<Output = SocketHandshake> + Send;

    /// Returns a constructed [`WithSocketMaker::WithSocket`] instance.
    fn make_with_socket(&self, with_socket: WithExaSocket) -> Self::WithSocket;
}
