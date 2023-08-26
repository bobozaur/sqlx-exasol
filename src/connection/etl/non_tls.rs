use std::fmt::Write;
use std::net::{IpAddr, SocketAddr, SocketAddrV4};

use arrayvec::ArrayString;
use futures_channel::oneshot::{self, Receiver, Sender};
use futures_core::future::BoxFuture;
use sqlx_core::error::Error as SqlxError;
use sqlx_core::net::{Socket, WithSocket};

use crate::connection::websocket::socket::{ExaSocket, WithExaSocket};

use super::get_etl_addr;

pub async fn spawn_non_tls_sockets(
    num_sockets: usize,
    ips: Vec<IpAddr>,
    port: u16,
) -> Result<
    Vec<(
        BoxFuture<'static, Result<ExaSocket, SqlxError>>,
        Receiver<SocketAddrV4>,
    )>,
    SqlxError,
> {
    tracing::trace!("spawning {num_sockets} non-TLS sockets");

    let mut output = Vec::with_capacity(num_sockets);

    for ip in ips.into_iter().take(num_sockets) {
        let mut ip_buf = ArrayString::<50>::new_const();
        write!(&mut ip_buf, "{ip}").expect("IP address should fit in 50 characters");
        let (tx, rx) = oneshot::channel();

        let wrapper = WithExaSocket(SocketAddr::new(ip, port));
        let with_socket = WithNonTlsSocket::new(wrapper, tx);
        let future = sqlx_core::net::connect_tcp(&ip_buf, port, with_socket).await?;

        output.push((future, rx));
    }

    Ok(output)
}

struct WithNonTlsSocket {
    wrapper: WithExaSocket,
    sender: Sender<SocketAddrV4>,
}

impl WithNonTlsSocket {
    fn new(wrapper: WithExaSocket, sender: Sender<SocketAddrV4>) -> Self {
        Self { wrapper, sender }
    }
}

impl WithSocket for WithNonTlsSocket {
    type Output = BoxFuture<'static, Result<ExaSocket, SqlxError>>;

    fn with_socket<S: Socket>(self, socket: S) -> Self::Output {
        let WithNonTlsSocket { wrapper, sender } = self;

        Box::pin(async move {
            let (socket, address) = get_etl_addr(socket).await?;
            sender
                .send(address)
                .map_err(|_| "could not send socket address for ETL job".to_owned())
                .map_err(SqlxError::Protocol)?;

            let socket = wrapper.with_socket(socket);
            Ok(socket)
        })
    }
}
