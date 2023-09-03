mod error;
mod export;
mod import;
mod non_tls;
#[cfg(any(feature = "etl_native_tls", feature = "etl_rustls"))]
mod tls;
mod traits;

use std::{
    io::{Error as IoError, Result as IoResult},
    net::{IpAddr, Ipv4Addr, SocketAddrV4},
};

use arrayvec::ArrayString;
pub use export::{ExaExport, ExportBuilder, QueryOrTable};
use futures_core::future::BoxFuture;
pub use import::{ExaImport, ImportBuilder, Trim};
use non_tls::non_tls_socket_spawners;
use sqlx_core::{error::Error as SqlxError, net::Socket};

use self::{error::ExaEtlError, traits::EtlJob};
use super::websocket::socket::ExaSocket;
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

async fn prepare<'a, 'c, T>(
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

    let (addrs, futures): (Vec<_>, Vec<_>) =
        socket_spawners(job.num_workers(), ips, port, with_tls)
            .await?
            .into_iter()
            .unzip();

    let query = job.query(addrs, with_tls, with_compression);
    let cmd = ExaCommand::new_execute(&query, &con.ws.attributes).try_into()?;
    con.ws.send(cmd).await?;

    let sockets = job.create_workers(futures, with_compression);

    let future = Box::pin(async move {
        let query_res: QueryResult = con.ws.recv::<Results>().await?.into();
        match query_res {
            QueryResult::ResultSet { .. } => Err(IoError::from(ExaEtlError::ResultSetFromEtl))?,
            QueryResult::RowCount { row_count } => Ok(ExaQueryResult::new(row_count)),
        }
    });

    Ok((future, sockets))
}

async fn socket_spawners(
    num: usize,
    ips: Vec<IpAddr>,
    port: u16,
    with_tls: bool,
) -> Result<Vec<(SocketAddrV4, SocketFuture)>, SqlxError> {
    let num_sockets = if num > 0 { num } else { ips.len() };

    #[cfg(any(feature = "etl_native_tls", feature = "etl_rustls"))]
    match with_tls {
        true => tls::tls_socket_spawners(num_sockets, ips, port).await,
        false => non_tls_socket_spawners(num_sockets, ips, port).await,
    }

    #[cfg(not(any(feature = "etl_native_tls", feature = "etl_rustls")))]
    match with_tls {
        true => Err(SqlxError::Tls("No ETL TLS feature set".into())),
        false => non_tls_socket_spawners(num_sockets, ips, port).await,
    }
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

#[derive(Debug, Clone, Copy)]
pub enum RowSeparator {
    LF,
    CR,
    CRLF,
}

impl AsRef<str> for RowSeparator {
    fn as_ref(&self) -> &str {
        match self {
            RowSeparator::LF => "LF",
            RowSeparator::CR => "CR",
            RowSeparator::CRLF => "CRLF",
        }
    }
}
