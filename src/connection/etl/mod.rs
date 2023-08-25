mod export;
mod import;
#[cfg(any(feature = "etl_native_tls", feature = "etl_rustls"))]
mod tls;

use std::fmt::Write;
use std::io::{Error as IoError, ErrorKind as IoErrorKind, Result as IoResult};
use std::net::{IpAddr, SocketAddr, SocketAddrV4};
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use arrayvec::ArrayString;
use futures_core::future::BoxFuture;
use futures_io::{AsyncRead, AsyncWrite};
use futures_util::io::BufReader;
use sqlx_core::error::Error as SqlxError;
use sqlx_core::net::{Socket, WithSocket};

use crate::connection::websocket::socket::WithExaSocket;
use crate::ExaDatabaseError;

use super::websocket::socket::ExaSocket;

pub use export::{ExaExport, ExportBuilder, QueryOrTable};
pub use import::{ExaImport, ImportBuilder, Trim};

/// Special Exasol packet that enables tunneling.
/// Exasol responds with an internal address that can be used in query.
const SPECIAL_PACKET: [u8; 12] = [2, 33, 33, 2, 1, 0, 0, 0, 1, 0, 0, 0];

const DOUBLE_CR_LF: &[u8; 4] = b"\r\n\r\n";

const GZ_FILE_EXT: &str = "gz";
const CSV_FILE_EXT: &str = "csv";

const HTTP_SCHEME: &str = "http";
const HTTPS_SCHEME: &str = "https";

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

async fn spawn_sockets(
    num: usize,
    ips: Vec<IpAddr>,
    port: u16,
    with_tls: bool,
) -> Result<Vec<(ExaSocket, SocketAddrV4)>, SqlxError> {
    let num_sockets = if num > 0 { num } else { ips.len() };

    #[cfg(any(feature = "etl_native_tls", feature = "etl_rustls"))]
    match with_tls {
        true => tls::spawn_tls_sockets(num_sockets, ips, port).await,
        false => {
            tracing::warn!("An ETL TLS feature is enabled but the connection is not using TLS. ETL will also not use it.");
            spawn_non_tls_sockets(num_sockets, ips, port).await
        }
    }

    #[cfg(not(any(feature = "etl_native_tls", feature = "etl_rustls")))]
    match with_tls {
        true => {
            let msg = "ETL with TLS requires the 'etl_native_tls' or 'etl_rustls' feature";
            Err(SqlxError::Tls(msg.into()))
        }
        false => spawn_non_tls_sockets(num_sockets, ips, port).await,
    }
}

fn append_filenames(
    query: &mut String,
    addrs: Vec<SocketAddrV4>,
    with_tls: bool,
    with_compression: bool,
) {
    let prefix = match with_tls {
        false => HTTP_SCHEME,
        true => HTTPS_SCHEME,
    };

    let ext = match with_compression {
        false => CSV_FILE_EXT,
        true => GZ_FILE_EXT,
    };

    for (idx, addr) in addrs.into_iter().enumerate() {
        let filename = format!("AT '{}://{}' FILE '{:0>5}.{}'\n", prefix, addr, idx, ext);
        query.push_str(&filename);
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

async fn spawn_non_tls_sockets(
    num_sockets: usize,
    ips: Vec<IpAddr>,
    port: u16,
) -> Result<Vec<(ExaSocket, SocketAddrV4)>, SqlxError> {
    tracing::trace!("spawning {num_sockets} non-TLS sockets");

    let mut sockets = Vec::with_capacity(num_sockets);

    for ip in ips.into_iter().take(num_sockets) {
        let mut ip_buf = ArrayString::<50>::new_const();
        write!(&mut ip_buf, "{ip}").expect("IP address should fit in 50 characters");

        let wrapper = WithExaSocket(SocketAddr::new(ip, port));
        let with_socket = WithNonTlsSocket(wrapper);
        let socket = sqlx_core::net::connect_tcp(&ip_buf, port, with_socket)
            .await?
            .await?;

        sockets.push(socket);
    }

    Ok(sockets)
}

struct WithNonTlsSocket(WithExaSocket);

impl WithSocket for WithNonTlsSocket {
    type Output = BoxFuture<'static, Result<(ExaSocket, SocketAddrV4), SqlxError>>;

    fn with_socket<S: Socket>(self, socket: S) -> Self::Output {
        let wrapper = self.0;

        Box::pin(async move {
            let (socket, address) = get_etl_addr(socket).await?;
            let socket = wrapper.with_socket(socket);
            Ok((socket, address))
        })
    }
}
