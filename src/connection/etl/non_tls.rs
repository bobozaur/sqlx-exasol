use std::{
    fmt::Write,
    io::Result as IoResult,
    net::{IpAddr, SocketAddr, SocketAddrV4},
};

use arrayvec::ArrayString;
use futures_core::future::BoxFuture;
use sqlx_core::{
    error::Error as SqlxError,
    net::{Socket, WithSocket},
};

use super::{get_etl_addr, SocketFuture};
use crate::connection::websocket::socket::{ExaSocket, WithExaSocket};

pub async fn non_tls_socket_spawners(
    num_sockets: usize,
    ips: Vec<IpAddr>,
    port: u16,
) -> Result<Vec<(SocketAddrV4, SocketFuture)>, SqlxError> {
    tracing::trace!("spawning {num_sockets} non-TLS sockets");

    let mut output = Vec::with_capacity(num_sockets);

    for ip in ips.into_iter().take(num_sockets) {
        let mut ip_buf = ArrayString::<50>::new_const();
        write!(&mut ip_buf, "{ip}").expect("IP address should fit in 50 characters");

        let wrapper = WithExaSocket(SocketAddr::new(ip, port));
        let with_socket = WithNonTlsSocket(wrapper);
        let (addr, future) = sqlx_core::net::connect_tcp(&ip_buf, port, with_socket)
            .await?
            .await?;

        output.push((addr, future));
    }

    Ok(output)
}

struct WithNonTlsSocket(WithExaSocket);

impl WithSocket for WithNonTlsSocket {
    type Output = BoxFuture<'static, Result<(SocketAddrV4, SocketFuture), SqlxError>>;

    fn with_socket<S: Socket>(self, socket: S) -> Self::Output {
        let wrapper = self.0;

        Box::pin(async move {
            let (socket, address) = get_etl_addr(socket).await?;

            let future: BoxFuture<IoResult<ExaSocket>> = Box::pin(async move {
                let socket = wrapper.with_socket(socket);
                Ok(socket)
            });

            Ok((address, future))
        })
    }
}
