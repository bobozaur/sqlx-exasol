mod export;
mod import;

use std::net::{IpAddr, SocketAddr};

use arrayvec::ArrayString;
use futures_util::{AsyncReadExt, AsyncWriteExt};
use sqlx_core::error::BoxDynError;

use crate::connection::{tls, websocket::socket::WithExaSocket};

use super::websocket::socket::ExaSocket;

/// Special Exasol packet that enables tunneling.
/// Exasol responds with an internal address that can be used in query.
const SPECIAL_PACKET: [u8; 12] = [2, 33, 33, 2, 1, 0, 0, 0, 1, 0, 0, 0];

/// Packet that ends tunnelling
const END_PACKET: &[u8; 5] = b"0\r\n\r\n";

/// Packet that tells Exasol the transport was successful
const SUCCESS_HEADERS: &[u8; 66] = b"HTTP/1.1 200 OK\r\n\
                                     Connection: close\r\n\
                                     Transfer-Encoding: chunked\r\n\
                                     \r\n";

/// Packet that tells Exasol the transport had an error
const ERROR_HEADERS: &[u8; 57] = b"HTTP/1.1 500 Internal Server Error\r\n\
                                   Connection: close\r\n\
                                   \r\n";

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

async fn make_worker(ip: IpAddr, port: u16) -> Result<(ExaSocket, SocketAddr), BoxDynError> {
    let host = ip.to_string();
    let socket_addr = SocketAddr::new(ip, port);
    let with_socket = WithExaSocket(socket_addr);

    let mut socket = sqlx_core::net::connect_tcp(&host, port, with_socket).await?;
    socket.write_all(&SPECIAL_PACKET).await?;

    // Read response buffer.
    let mut buf = [0; 24];
    socket.read_exact(&mut buf).await?;
    let address = parse_address(buf)?;

    Ok((socket, address))
}

async fn upgrade_worker() {
    let tls_opts = todo!();
    // let (socket, _) = tls::maybe_upgrade(socket, &host, tls_opts).await?;
    todo!();
}

/// Parses response to return the internal Exasol address to be used in query.
fn parse_address(buf: [u8; 24]) -> Result<SocketAddr, BoxDynError> {
    let port = u16::from_le_bytes([buf[4], buf[5]]);

    let mut str_addr = ArrayString::<15>::new_const();
    buf[8..]
        .iter()
        .take_while(|b| **b != b'\0')
        .for_each(|b| str_addr.push(char::from(*b)));

    let ip = str_addr.as_str().parse()?;
    Ok(SocketAddr::new(ip, port))
}
