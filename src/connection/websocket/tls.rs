use sqlx_core::net::{
    tls::{self, TlsConfig},
    Socket, WithSocket,
};

use super::socket::{ExaSocket, WithExaSocket};
use crate::{options::ExaTlsOptionsRef, ExaSslMode, SqlxResult};

/// Implementor of [`WithSocket`] that encapsulates the TLS/non-TLS decision logic.
pub struct WithMaybeTlsExaSocket<'a> {
    wrapper: WithExaSocket,
    host: &'a str,
    tls_opts: ExaTlsOptionsRef<'a>,
}

impl<'a> WithMaybeTlsExaSocket<'a> {
    pub fn new(wrapper: WithExaSocket, host: &'a str, tls_opts: ExaTlsOptionsRef<'a>) -> Self {
        Self {
            wrapper,
            host,
            tls_opts,
        }
    }
}

impl WithSocket for WithMaybeTlsExaSocket<'_> {
    type Output = SqlxResult<(ExaSocket, bool)>;

    async fn with_socket<S: Socket>(self, socket: S) -> Self::Output {
        let WithMaybeTlsExaSocket {
            wrapper,
            host,
            tls_opts,
        } = self;

        match tls_opts.ssl_mode {
            ExaSslMode::Disabled => {
                return Ok((wrapper.with_socket(socket).await, false));
            }

            ExaSslMode::Preferred => {
                if !tls::available() {
                    tracing::debug!("not performing TLS upgrade: TLS support not compiled in");
                    return Ok((wrapper.with_socket(socket).await, false));
                }
            }

            ExaSslMode::Required | ExaSslMode::VerifyIdentity | ExaSslMode::VerifyCa => {
                tls::error_if_unavailable()?;
            }
        }

        let accept_invalid_certs = !matches!(
            tls_opts.ssl_mode,
            ExaSslMode::VerifyCa | ExaSslMode::VerifyIdentity
        );

        let tls_config = TlsConfig {
            accept_invalid_certs,
            accept_invalid_hostnames: !matches!(tls_opts.ssl_mode, ExaSslMode::VerifyIdentity),
            hostname: host,
            root_cert_path: tls_opts.ssl_ca,
            client_cert_path: tls_opts.ssl_client_cert,
            client_key_path: tls_opts.ssl_client_key,
        };

        tls::handshake(socket, tls_config, wrapper)
            .await
            .map(|s| (s, true))
    }
}
