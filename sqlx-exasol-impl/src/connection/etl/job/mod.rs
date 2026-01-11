pub mod maybe_tls;

use std::{
    fmt::Write,
    future::Future,
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4},
};

use arrayvec::ArrayString;
use futures_core::future::BoxFuture;
use sqlx_core::{
    net::{Socket, WithSocket},
    sql_str::{AssertSqlSafe, SqlSafeStr},
};

use crate::{
    connection::websocket::{
        future::{ExaRoundtrip, GetHosts, WebSocketFuture},
        request::Execute,
        socket::{ExaSocket, WithExaSocket},
    },
    etl::{error::ExaEtlError, job::maybe_tls::WithMaybeTlsSocketMaker, EtlQuery, ExecuteEtl},
    ExaConnection, SqlxResult,
};

/// Alias for a future that sets up a socket to be used by a worker. Typically means doing the TLS
/// handshake.
///
/// Still used in non-TLS contexts as it makes matters easier to reason about. In non-TLS cases, it
/// ends up being an instantly ready future, since there's no handshake.
pub type SocketSetup = BoxFuture<'static, io::Result<ExaSocket>>;

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

    fn create_worker(&self, setup_future: SocketSetup, with_compression: bool) -> Self::Worker;

    /// Connects a worker for each IP address provided.
    /// The internal socket addresses of the corresponding Exasol nodes are provided alongside the
    /// workers to be used in query generation.
    fn connect_workers(
        &self,
        with_socket_maker: impl WithSocketMaker,
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
                let with_socket = with_socket_maker.make_with_socket(with_exa_socket);
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
            let num = self.num_workers();

            let ips = GetHosts::new(socket_addr.ip())
                .future(&mut conn.ws)
                .await?
                .into();

            let with_tls = conn.attributes().encryption_enabled();
            let with_compression = self
                .use_compression()
                .unwrap_or(conn.attributes().compression_enabled());
            let with_worker = WithMaybeTlsSocketMaker::new(with_tls)?;

            // Get the internal Exasol node addresses and the workers
            let (addrs, workers) = self
                .connect_workers(with_worker, num, ips, port, with_compression)
                .await?;

            // Query execution driving future to be returned and awaited alongside the worker IO
            let query = AssertSqlSafe(self.query(addrs, with_tls, with_compression)).into_sql_str();
            let future = ExecuteEtl(ExaRoundtrip::new(Execute(query))).future(&mut conn.ws);

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

/// Trait used as an interface for constructing a type implementing [`WithSocket`] that outputs a
/// future which sets up a socket to be used by an ETL worker.
///
/// One purpose of this trait is to abstract away the TLS/non-TLS workers. In TLS scenarios, we
/// might be connecting multiple sockets with the same TLS config.
pub trait WithSocketMaker: Send + Sync {
    type WithSocket: WithSocket<Output = SocketSetup> + Send;

    /// Returns a constructed [`WithSocketMaker::WithSocket`] instance.
    fn make_with_socket(&self, with_socket: WithExaSocket) -> Self::WithSocket;
}

impl<T> WithSocketMaker for Box<T>
where
    T: WithSocketMaker,
{
    type WithSocket = T::WithSocket;

    fn make_with_socket(&self, with_socket: WithExaSocket) -> Self::WithSocket {
        (**self).make_with_socket(with_socket)
    }
}

/// Type for a wrapper [`WithSocket`] implementation that also retrieves and returns the socket
/// address.
///
/// Behind the scenes Exasol will import/export to a file located on the one-shot HTTP server we
/// will host on the socket.
///
/// The "file" will be defined something like <`http://10.25.0.2/0001.csv`>.
///
/// While I don't know the exact implementation details, Exasol seems to set a proxy to the socket
/// we connect and this wrapper is used to retrieve the internal IP of that proxy so we can
/// construct the file name.
#[derive(Debug)]
struct WithSocketAddr<T>(pub T)
where
    T: WithSocket;

impl<T> WithSocket for WithSocketAddr<T>
where
    T: WithSocket<Output = SocketSetup> + Send,
{
    type Output = SqlxResult<(SocketAddrV4, SocketSetup)>;

    async fn with_socket<S: Socket>(self, mut socket: S) -> Self::Output {
        /// Special Exasol packet that enables tunneling.
        /// Exasol responds with an internal address that can be used in a query.
        const SPECIAL_PACKET: [u8; 12] = [2, 33, 33, 2, 1, 0, 0, 0, 1, 0, 0, 0];

        // Write special packet
        let mut write_start = 0;

        while write_start < SPECIAL_PACKET.len() {
            let written = socket.write(&SPECIAL_PACKET[write_start..]).await?;
            write_start += written;
        }

        // Read response buffer.
        let mut buf = [0; 24];
        let mut read_start = 0;

        while read_start < buf.len() {
            let mut buf = &mut buf[read_start..];
            let read = socket.read(&mut buf).await?;
            read_start += read;
        }

        // Parse address
        let mut ip_buf = ArrayString::<16>::new_const();

        buf[8..]
            .iter()
            .take_while(|b| **b != b'\0')
            .for_each(|b| ip_buf.push(char::from(*b)));

        let port = u16::from_le_bytes([buf[4], buf[5]]);
        let ip = ip_buf
            .parse::<Ipv4Addr>()
            .map_err(ExaEtlError::from)
            .map_err(io::Error::from)?;
        let address = SocketAddrV4::new(ip, port);

        Ok((address, self.0.with_socket(socket).await))
    }
}
