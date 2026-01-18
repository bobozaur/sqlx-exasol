use std::{
    io,
    net::{Ipv4Addr, SocketAddrV4},
};

use sqlx_core::net::{Socket, WithSocket};

use crate::{
    etl::{error::ExaEtlError, job::SocketHandshake},
    SqlxResult,
};

/// Type for a wrapper [`WithSocket`] implementation that also retrieves and returns the socket
/// address.
///
/// Behind the scenes Exasol will import/export to a file located on the one-shot HTTP server we
/// will host on the socket.
///
/// The "file" will be defined something like <`http://10.25.0.2/0001.csv`>.
///
/// While I don't know the exact implementation details, Exasol seems to set a proxy to the socket
/// we connect and this wrapper is used to retrieve the internal IP of that proxy so we can use it
/// when generating the `IMPORT`/`EXPORT` query.
#[derive(Debug)]
pub struct WithSocketAddr<T>(pub T)
where
    T: WithSocket;

impl<T> WithSocket for WithSocketAddr<T>
where
    T: WithSocket<Output = SocketHandshake> + Send,
{
    type Output = SqlxResult<(SocketAddrV4, SocketHandshake)>;

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
        let ip_buf = buf[8..]
            .split(|b| *b == b'\0')
            .next()
            .expect("at least one slice");

        let ip = std::str::from_utf8(ip_buf)
            .map_err(ExaEtlError::from)
            .map_err(io::Error::from)?
            .parse::<Ipv4Addr>()
            .map_err(ExaEtlError::from)
            .map_err(io::Error::from)?;

        let port = u16::from_le_bytes([buf[4], buf[5]]);
        let address = SocketAddrV4::new(ip, port);

        Ok((address, self.0.with_socket(socket).await))
    }
}
