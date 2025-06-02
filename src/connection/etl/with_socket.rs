use std::{
    io,
    net::{Ipv4Addr, SocketAddrV4},
};

use arrayvec::ArrayString;
use sqlx_core::net::{Socket, WithSocket};

use crate::{
    connection::websocket::socket::WithExaSocket,
    etl::{error::ExaEtlError, WithSocketFuture},
    SqlxResult,
};

/// Trait used as an interface for constructing a type implementing [`WithSocket`]
/// that outputs a socket spawning future.
///
/// The constructed future can then be awaited regardless of its origin, bridging the TLS/non-TLS
/// code.
pub trait WithSocketMaker: Send + Sync {
    type WithSocket: WithSocket<Output = SqlxResult<WithSocketFuture>> + Send;

    fn make_with_socket(&self, with_socket: WithExaSocket) -> Self::WithSocket;
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
pub struct WithSocketAddr<T>(pub T)
where
    T: WithSocket;

impl<T> WithSocket for WithSocketAddr<T>
where
    T: WithSocket<Output = SqlxResult<WithSocketFuture>> + Send,
{
    type Output = SqlxResult<(SocketAddrV4, WithSocketFuture)>;

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

        self.0.with_socket(socket).await.map(|f| (address, f))
    }
}
