use sqlx_core::{
    net::tls::{self, TlsConfig},
    Error as SqlxError,
};

use crate::{
    connection::websocket::socket::{ExaSocket, WithExaSocket},
    options::{ExaConnectOptionsRef, ExaSslMode},
};

pub(crate) async fn maybe_upgrade(
    socket: ExaSocket,
    host: &str,
    options: ExaConnectOptionsRef<'_>,
) -> Result<(ExaSocket, bool), SqlxError> {
    match options.ssl_mode {
        ExaSslMode::Disabled => {
            return Ok((socket, false));
        }

        ExaSslMode::Preferred => {
            if !tls::available() {
                tracing::debug!("not performing TLS upgrade: TLS support not compiled in");
                return Ok((socket, false));
            }
        }

        ExaSslMode::Required | ExaSslMode::VerifyIdentity | ExaSslMode::VerifyCa => {
            tls::error_if_unavailable()?;
        }
    }

    let accept_invalid_certs = !matches!(
        options.ssl_mode,
        ExaSslMode::VerifyCa | ExaSslMode::VerifyIdentity
    );

    let tls_config = TlsConfig {
        accept_invalid_certs,
        accept_invalid_hostnames: !matches!(options.ssl_mode, ExaSslMode::VerifyIdentity),
        hostname: host,
        root_cert_path: options.ssl_ca,
        client_cert_path: options.ssl_client_cert,
        client_key_path: options.ssl_client_key,
    };

    let with_socket = WithExaSocket(socket.ip_addr);
    let socket = socket.inner;

    tls::handshake(socket, tls_config, with_socket)
        .await
        .map(|s| (s, true))
}
