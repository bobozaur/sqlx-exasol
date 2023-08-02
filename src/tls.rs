use sqlx_core::{
    net::{
        tls::{self, TlsConfig},
        Socket, WithSocket,
    },
    Error as SqlxError,
};

use crate::{
    options::{ExaConnectOptionsRef, ExaSslMode},
    websocket::{RwSocket, WithRwSocket},
};

pub(crate) async fn maybe_upgrade<S: Socket>(
    socket: S,
    host: &str,
    options: ExaConnectOptionsRef<'_>,
) -> Result<(RwSocket, bool), SqlxError> {
    match options.ssl_mode {
        ExaSslMode::Disabled => {
            let socket = WithRwSocket::with_socket(WithRwSocket, socket);
            return Ok((socket, false));
        }

        ExaSslMode::Preferred => {
            if !tls::available() {
                tracing::debug!("not performing TLS upgrade: TLS support not compiled in");
                let socket = WithRwSocket::with_socket(WithRwSocket, socket);
                return Ok((socket, false));
            }
        }

        ExaSslMode::Required | ExaSslMode::VerifyIdentity | ExaSslMode::VerifyCa => {
            tls::error_if_unavailable()?;
        }
    }

    let tls_config = TlsConfig {
        accept_invalid_certs: !matches!(
            options.ssl_mode,
            ExaSslMode::VerifyCa | ExaSslMode::VerifyIdentity
        ),
        accept_invalid_hostnames: !matches!(options.ssl_mode, ExaSslMode::VerifyIdentity),
        hostname: host,
        root_cert_path: options.ssl_ca,
        client_cert_path: options.ssl_client_cert,
        client_key_path: options.ssl_client_key,
    };

    tls::handshake(socket, tls_config, WithRwSocket)
        .await
        .map(|s| (s, true))
}
