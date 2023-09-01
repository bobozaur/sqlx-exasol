use std::fmt::Write;
use std::io::Result as IoResult;
use std::net::{IpAddr, SocketAddr, SocketAddrV4};

use arrayvec::ArrayString;
use futures_core::future::BoxFuture;
use sqlx_core::error::Error as SqlxError;
use sqlx_core::net::{Socket, WithSocket};

use crate::connection::websocket::socket::{ExaSocket, WithExaSocket};

use super::get_etl_addr;

pub async fn non_tls_socket_spawners(
    num_sockets: usize,
    ips: Vec<IpAddr>,
    port: u16,
) -> Result<
    Vec<
        BoxFuture<
            'static,
            Result<(SocketAddrV4, BoxFuture<'static, IoResult<ExaSocket>>), SqlxError>,
        >,
    >,
    SqlxError,
> {
    tracing::trace!("spawning {num_sockets} non-TLS sockets");

    let mut output = Vec::with_capacity(num_sockets);

    for ip in ips.into_iter().take(num_sockets) {
        let mut ip_buf = ArrayString::<50>::new_const();
        write!(&mut ip_buf, "{ip}").expect("IP address should fit in 50 characters");

        let wrapper = WithExaSocket(SocketAddr::new(ip, port));
        let with_socket = WithNonTlsSocket(wrapper);
        let future = sqlx_core::net::connect_tcp(&ip_buf, port, with_socket).await?;

        output.push(future);
    }

    Ok(output)
}

struct WithNonTlsSocket(WithExaSocket);

impl WithSocket for WithNonTlsSocket {
    type Output = BoxFuture<
        'static,
        Result<(SocketAddrV4, BoxFuture<'static, IoResult<ExaSocket>>), SqlxError>,
    >;

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
