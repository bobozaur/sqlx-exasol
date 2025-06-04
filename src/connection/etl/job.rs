use std::{
    fmt::Write,
    future::Future,
    net::{IpAddr, SocketAddr, SocketAddrV4},
};

use arrayvec::ArrayString;

use crate::{
    connection::websocket::{
        future::{ExaRoundtrip, GetHosts, WebSocketFuture},
        request::Execute,
        socket::WithExaSocket,
    },
    etl::{
        non_tls::WithNonTlsWorker,
        tls,
        with_worker::{WithSocketAddr, WithWorker},
        EtlQuery, ExecuteEtl, WithSocketFuture,
    },
    ExaConnection, SqlxResult,
};

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

    fn use_compression(&self) -> Option<bool>;

    fn num_workers(&self) -> usize;

    fn create_worker(&self, future: WithSocketFuture, with_compression: bool) -> Self::Worker;

    /// Connects a worker for each IP address provided.
    /// The internal socket addresses of the corresponding Exasol nodes are provided alongside the
    /// workers to be used in query generation.
    fn connect_workers(
        &self,
        make_socket: impl WithWorker,
        num: usize,
        ips: Vec<IpAddr>,
        port: u16,
        with_compression: bool,
    ) -> impl Future<Output = SqlxResult<(Vec<SocketAddrV4>, Vec<Self::Worker>)>> + Send {
        async move {
            let num_sockets = if num > 0 { num } else { ips.len() };

            let mut addrs = Vec::with_capacity(num_sockets);
            let mut workers = Vec::with_capacity(num_sockets);

            for ip in ips.into_iter().take(num_sockets) {
                let mut ip_buf = ArrayString::<50>::new_const();
                write!(&mut ip_buf, "{ip}").expect("IP address should fit in 50 characters");

                let with_exa_socket = WithExaSocket(SocketAddr::new(ip, port));
                let with_socket = make_socket.make_with_socket(with_exa_socket);
                let (addr, future) =
                    sqlx_core::net::connect_tcp(&ip_buf, port, WithSocketAddr(with_socket))
                        .await??;
                let worker = self.create_worker(future, with_compression);

                addrs.push(addr);
                workers.push(worker);
            }

            Ok((addrs, workers))
        }
    }

    fn query(&self, addrs: Vec<SocketAddrV4>, with_tls: bool, with_compression: bool) -> String;

    /// Builds an ETL job comprising of a [`EtlQuery`], that drives the execution of the ETL query,
    /// and an array of workers that perform the IO.
    fn build_etl<'a, 'c>(
        &'a self,
        conn: &'c mut ExaConnection,
    ) -> impl Future<Output = SqlxResult<(EtlQuery<'c>, Vec<Self::Worker>)>> + Send
    where
        'c: 'a,
    {
        async {
            let socket_addr = conn.server();
            let port = socket_addr.port();
            let num = self.num_workers();

            let ips = GetHosts::new(socket_addr.ip())
                .future(&mut conn.ws)
                .await?
                .into();

            let with_tls = conn.attributes().encryption_enabled();
            let with_compression = self
                .use_compression()
                .unwrap_or(conn.attributes().compression_enabled());

            // Get the internal Exasol node addresses and the workers
            let (addrs, workers) = if with_tls {
                self.connect_workers(tls::with_worker()?, num, ips, port, with_compression)
                    .await?
            } else {
                self.connect_workers(WithNonTlsWorker, num, ips, port, with_compression)
                    .await?
            };

            // Query execution driving future to be returned and awaited alongside the worker IO
            let query = self.query(addrs, with_tls, with_compression);
            let future = ExecuteEtl(ExaRoundtrip::new(Execute(query.into()))).future(&mut conn.ws);

            Ok((EtlQuery(future), workers))
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
