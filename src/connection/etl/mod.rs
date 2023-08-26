mod error;
mod export;
mod import;
mod non_tls;
#[cfg(any(feature = "etl_native_tls", feature = "etl_rustls"))]
mod tls;
mod traits;

use std::net::{IpAddr, SocketAddrV4};

use crate::{
    error::{ExaProtocolError, ExaResultExt},
    ExaConnection, ExaDatabaseError, ExaQueryResult,
};
use arrayvec::ArrayString;
use futures_channel::oneshot::Receiver;
use futures_core::future::BoxFuture;
use futures_util::future::{select, try_join_all, Either};
use sqlx_core::error::Error as SqlxError;
use sqlx_core::net::Socket;

use non_tls::non_tls_socket_spawners;

use self::traits::EtlJob;

use super::websocket::socket::ExaSocket;

pub use export::{ExaExport, ExportBuilder, QueryOrTable};
pub use import::{ExaImport, ImportBuilder, Trim};

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

type SocketSpawner = BoxFuture<'static, Result<ExaSocket, SqlxError>>;

/// Type of the future that executes the ETL job.
pub type JobFuture<'a> = BoxFuture<'a, Result<ExaQueryResult, SqlxError>>;

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

    let (futures, rxs): (Vec<_>, Vec<_>) = socket_spawners(job.num_workers(), ips, port, with_tls)
        .await?
        .into_iter()
        .unzip();

    let addrs_fut = try_join_all(rxs);
    let sockets_fut = try_join_all(futures);

    let (addrs, sockets_fut): (Vec<_>, BoxFuture<'_, Result<Vec<ExaSocket>, SqlxError>>) =
        match select(addrs_fut, sockets_fut).await {
            Either::Left((addrs, sockets_fut)) => (addrs.to_sqlx_err()?, Box::pin(sockets_fut)),
            Either::Right((sockets, addrs_fut)) => (
                addrs_fut.await.to_sqlx_err()?,
                Box::pin(async move { sockets }),
            ),
        };

    let query = job.query(addrs, with_tls, with_compression);
    let query_fut: JobFuture = Box::pin(con.execute_etl(query));

    let (sockets, query_fut) = match select(query_fut, sockets_fut).await {
        Either::Right((sockets, f)) => (sockets?, f),
        Either::Left((res, _)) => match res {
            Ok(_) => Err(ExaProtocolError::EtlUnexpectedEnd)?,
            Err(e) => Err(e)?,
        },
    };

    let sockets = job.create_workers(sockets, with_compression);

    Ok((query_fut, sockets))
}

async fn socket_spawners(
    num: usize,
    ips: Vec<IpAddr>,
    port: u16,
    with_tls: bool,
) -> Result<Vec<(SocketSpawner, Receiver<SocketAddrV4>)>, SqlxError> {
    let num_sockets = if num > 0 { num } else { ips.len() };

    #[cfg(any(feature = "etl_native_tls", feature = "etl_rustls"))]
    match with_tls {
        true => tls::tls_socket_spawners(num_sockets, ips, port).await,
        false => non_tls_socket_spawners(num_sockets, ips, port).await,
    }

    #[cfg(not(any(feature = "etl_native_tls", feature = "etl_rustls")))]
    match with_tls {
        true => Err(SqlxError::Tls("A TLS feature is required for ETL TLS")),
        false => non_tls_socket_spawners(num_sockets, ips, port).await,
    }
}

/// Behind the scenes Exasol will import/export to a file located on the
/// one-shot HTTP server we will host on this socket.
///
/// The "file" will be defined something like "http://10.25.0.2/0001.csv".
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
    let ip = ip_buf.parse().map_err(ExaDatabaseError::unknown)?;
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