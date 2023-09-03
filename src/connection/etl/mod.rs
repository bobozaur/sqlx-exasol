mod error;
mod export;
mod import;
mod non_tls;
mod row_separator;
#[cfg(any(feature = "etl_native_tls", feature = "etl_rustls"))]
mod tls;
mod traits;

use std::{
    fmt::Write as _,
    io::{Error as IoError, Result as IoResult},
    net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4},
};

use arrayvec::ArrayString;
pub use export::{ExaExport, ExportBuilder, ExportSource};
use futures_core::future::BoxFuture;
pub use import::{ExaImport, ImportBuilder, Trim};
pub use row_separator::RowSeparator;
use sqlx_core::{error::Error as SqlxError, net::Socket};

use self::{
    error::ExaEtlError,
    non_tls::NonTlsSocketSpawner,
    traits::{EtlJob, WithSocketMaker},
};
use super::websocket::socket::{ExaSocket, WithExaSocket};
use crate::{
    command::ExaCommand,
    responses::{QueryResult, Results},
    ExaConnection, ExaQueryResult,
};

/// Special Exasol packet that enables tunneling.
/// Exasol responds with an internal address that can be used in query.
const SPECIAL_PACKET: [u8; 12] = [2, 33, 33, 2, 1, 0, 0, 0, 1, 0, 0, 0];

/// We do some implicit buffering as we have to parse
/// the incoming HTTP request and ignore the headers, read chunk sizes, etc.
///
/// We do that by reading one byte at a time and keeping track
/// of what we read to walk through states.
///
/// It would be higly inefficient to read a single byte from the
/// TCP stream every time, so we instead use a small [`futures_util::io::BufReader`].
const IMPLICIT_BUFFER_CAP: usize = 128;

/// Type of the future that executes the ETL job.
type JobFuture<'a> = BoxFuture<'a, Result<ExaQueryResult, SqlxError>>;
type SocketFuture = BoxFuture<'static, IoResult<ExaSocket>>;

/// Builds an ETL job comprising of a [`JobFuture`], that drives the execution
/// of the ETL query, and an array of workers that perform the IO.
async fn build_etl<'a, 'c, T>(
    job: &'a T,
    con: &'c mut ExaConnection,
) -> Result<(JobFuture<'c>, Vec<T::Worker>), SqlxError>
where
    T: EtlJob,
    'c: 'a,
{
    let ips = con.ws.get_hosts().await?;
    let port = con.ws.socket_addr().port();
    let with_tls = con.attributes().encryption_enabled;
    let with_compression = job
        .use_compression()
        .unwrap_or(con.attributes().compression_enabled);

    // Get the internal Exasol node addresses and the socket spawning futures
    let (addrs, futures): (Vec<_>, Vec<_>) =
        socket_spawners(job.num_workers(), ips, port, with_tls)
            .await?
            .into_iter()
            .unzip();

    // Construct and send query
    let query = job.query(addrs, with_tls, with_compression);
    let cmd = ExaCommand::new_execute(&query, &con.ws.attributes).try_into()?;
    con.ws.send(cmd).await?;

    // Create the ETL workers
    let sockets = job.create_workers(futures, with_compression);

    // Query execution driving future to be returned and awaited
    // alongside the worker IO operations
    let future = Box::pin(async move {
        let query_res: QueryResult = con.ws.recv::<Results>().await?.into();
        match query_res {
            QueryResult::ResultSet { .. } => Err(IoError::from(ExaEtlError::ResultSetFromEtl))?,
            QueryResult::RowCount { row_count } => Ok(ExaQueryResult::new(row_count)),
        }
    });

    Ok((future, sockets))
}

/// Wrapper over [`_socket_spawners`] used to handle TLS/non-TLS reasoning.
async fn socket_spawners(
    num: usize,
    ips: Vec<IpAddr>,
    port: u16,
    with_tls: bool,
) -> Result<Vec<(SocketAddrV4, SocketFuture)>, SqlxError> {
    let num_sockets = if num > 0 { num } else { ips.len() };

    #[cfg(any(feature = "etl_native_tls", feature = "etl_rustls"))]
    match with_tls {
        true => _socket_spawners(tls::tls_with_socket_maker()?, num_sockets, ips, port).await,
        false => _socket_spawners(NonTlsSocketSpawner, num_sockets, ips, port).await,
    }

    #[cfg(not(any(feature = "etl_native_tls", feature = "etl_rustls")))]
    match with_tls {
        true => Err(SqlxError::Tls("No ETL TLS feature set".into())),
        false => _socket_spawners(NonTlsSocketSpawner, num_sockets, ips, port).await,
    }
}

/// Creates a socket making future for each IP address provided.
/// The internal socket address of the corresponding Exasol node
/// is provided alongside the future, to be used in query generation.
async fn _socket_spawners<T>(
    socket_spawner: T,
    num_sockets: usize,
    ips: Vec<IpAddr>,
    port: u16,
) -> Result<Vec<(SocketAddrV4, SocketFuture)>, SqlxError>
where
    T: WithSocketMaker,
{
    let mut output = Vec::with_capacity(num_sockets);

    for ip in ips.into_iter().take(num_sockets) {
        let mut ip_buf = ArrayString::<50>::new_const();
        write!(&mut ip_buf, "{ip}").expect("IP address should fit in 50 characters");

        let wrapper = WithExaSocket(SocketAddr::new(ip, port));
        let with_socket = socket_spawner.make_with_socket(wrapper);
        let (addr, future) = sqlx_core::net::connect_tcp(&ip_buf, port, with_socket)
            .await?
            .await?;

        output.push((addr, future));
    }

    Ok(output)
}

/// Behind the scenes Exasol will import/export to a file located on the
/// one-shot HTTP server we will host on this socket.
///
/// The "file" will be defined something like <`http://10.25.0.2/0001.csv`>.
///
/// While I don't know the exact implementation details, I assume Exasol
/// does port forwarding to/from the socket we connect (the one in this function)
/// and a local socket it opens (which has the address used in the file).
///
/// This function is used to retrieve the internal IP of that local socket,
/// so we can construct the file name.
async fn get_etl_addr<S>(mut socket: S) -> Result<(S, SocketAddrV4), SqlxError>
where
    S: Socket,
{
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
        .map_err(IoError::from)?;
    let address = SocketAddrV4::new(ip, port);

    Ok((socket, address))
}
