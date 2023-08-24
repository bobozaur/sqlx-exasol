mod export;
mod import;
#[cfg(any(feature = "etl_native_tls", feature = "etl_rustls"))]
mod tls;

use std::io::{Error as IoError, ErrorKind as IoErrorKind, Result as IoResult};
use std::iter;
use std::net::{IpAddr, SocketAddr, SocketAddrV4};
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use arrayvec::ArrayString;
use futures_io::{AsyncRead, AsyncWrite};
use futures_util::future::try_join_all;
use futures_util::{io::BufReader, AsyncReadExt, AsyncWriteExt};
use sqlx_core::error::{BoxDynError, Error as SqlxError};

use crate::connection::websocket::socket::WithExaSocket;
use crate::ExaDatabaseError;

use super::websocket::socket::ExaSocket;

pub use export::{ExaExport, ExportOptions, QueryOrTable};
pub use import::{ExaImport, ImportOptions, Trim};

/// Special Exasol packet that enables tunneling.
/// Exasol responds with an internal address that can be used in query.
const SPECIAL_PACKET: [u8; 12] = [2, 33, 33, 2, 1, 0, 0, 0, 1, 0, 0, 0];

const DOUBLE_CR_LF: &[u8; 4] = b"\r\n\r\n";

const GZ_FILE_EXT: &str = "gz";
const CSV_FILE_EXT: &str = "csv";

const HTTP_PREFIX: &str = "http";
const HTTPS_PREFIX: &str = "https";

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

fn poll_read_byte(socket: Pin<&mut BufReader<ExaSocket>>, cx: &mut Context) -> Poll<IoResult<u8>> {
    let mut buffer = [0; 1];
    let n = ready!(socket.poll_read(cx, &mut buffer))?;

    if n != 1 {
        let msg = "cannot read chunk byte";
        let err = IoError::new(IoErrorKind::InvalidInput, msg);
        Poll::Ready(Err(err))
    } else {
        Poll::Ready(Ok(buffer[0]))
    }
}

/// Sends some static data, returning whether all of it was sent or not.
fn poll_send_static(
    socket: Pin<&mut BufReader<ExaSocket>>,
    cx: &mut Context<'_>,
    buf: &'static [u8],
    offset: &mut usize,
) -> Poll<IoResult<bool>> {
    if *offset >= buf.len() {
        return Poll::Ready(Ok(true));
    }

    let num_bytes = ready!(socket.poll_write(cx, &buf[*offset..]))?;
    *offset += num_bytes;

    Poll::Ready(Ok(false))
}

fn poll_until_double_crlf(
    socket: Pin<&mut BufReader<ExaSocket>>,
    cx: &mut Context,
    buf: &mut [u8; 4],
) -> Poll<IoResult<bool>> {
    let byte = ready!(poll_read_byte(socket, cx))?;

    // Shift bytes
    buf[0] = buf[1];
    buf[1] = buf[2];
    buf[2] = buf[3];
    buf[3] = byte;

    // If true, all headers have been read
    match buf == DOUBLE_CR_LF {
        true => Poll::Ready(Ok(true)),
        false => Poll::Ready(Ok(false)),
    }
}

async fn start_jobs(
    num_jobs: usize,
    ips: Vec<IpAddr>,
    port: u16,
    encrypted: bool,
) -> Result<Vec<(ExaSocket, SocketAddrV4)>, SqlxError> {
    let num_jobs = if num_jobs > 0 { num_jobs } else { ips.len() };
    let futures = ips
        .into_iter()
        .take(num_jobs)
        .map(|ip| spawn_socket(ip, port));

    let (sockets, addrs): (Vec<ExaSocket>, Vec<SocketAddrV4>) = try_join_all(futures)
        .await
        .map_err(|e| ExaDatabaseError::unknown(e.to_string()))?
        .into_iter()
        .unzip();

    tracing::info!("before sockets");

    let sockets = match encrypted {
        #[cfg(any(feature = "etl_native_tls", feature = "etl_rustls"))]
        true => {
            let cert = tls::make_cert()?;
            let futures = sockets.into_iter().map(|s| tls::upgrade(s, &cert));
            try_join_all(futures).await?
        }
        _ => sockets,
    };

    tracing::info!("after sockets");

    Ok(iter::zip(sockets, addrs).collect())
}

async fn spawn_socket(ip: IpAddr, port: u16) -> Result<(ExaSocket, SocketAddrV4), BoxDynError> {
    let host = ip.to_string();
    let socket_addr = SocketAddr::new(ip, port);
    let with_socket = WithExaSocket(socket_addr);

    let mut socket = sqlx_core::net::connect_tcp(&host, port, with_socket).await?;
    socket.write_all(&SPECIAL_PACKET).await?;

    // Read response buffer.
    let mut buf = [0; 24];
    socket.read_exact(&mut buf).await?;
    let mut ip_buf = ArrayString::<16>::new_const();

    buf[8..]
        .iter()
        .take_while(|b| **b != b'\0')
        .for_each(|b| ip_buf.push(char::from(*b)));

    let port = u16::from_le_bytes([buf[4], buf[5]]);
    let ip = ip_buf.parse()?;
    let address = SocketAddrV4::new(ip, port);

    Ok((socket, address))
}

fn append_filenames(
    query: &mut String,
    addrs: Vec<SocketAddrV4>,
    is_encrypted: bool,
    is_compressed: bool,
) {
    let prefix = match is_encrypted {
        false => HTTP_PREFIX,
        true => HTTPS_PREFIX,
    };

    let ext = match is_compressed {
        false => CSV_FILE_EXT,
        true => GZ_FILE_EXT,
    };

    for (idx, addr) in addrs.into_iter().enumerate() {
        let filename = format!("AT '{}://{}' FILE '{:0>5}.{}'\n", prefix, addr, idx, ext);
        query.push_str(&filename);
    }
}
