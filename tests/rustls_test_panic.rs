/// Testing the rustls features.
/// Unfortunately, not that much to do yet since rustls wants proper hostnames and not IP addresses.
/// Testing locally is not really great in this regard.
/// But this is mainly to test that the code compiles with the given rustls features.

#[cfg(test)]
#[cfg(all(feature = "rustls", feature = "flate2"))]
#[allow(unused)]
mod tests {
    use __rustls::client::{ServerCertVerified, ServerCertVerifier};
    use __rustls::{Certificate, ClientConfig, Error, ServerName};
    use exasol::error::Result;
    use exasol::*;
    use std::env;
    use std::sync::Arc;
    use std::time::SystemTime;

    struct CertVerifier;

    impl ServerCertVerifier for CertVerifier {
        fn verify_server_cert(
            &self,
            end_entity: &Certificate,
            intermediates: &[Certificate],
            server_name: &ServerName,
            scts: &mut dyn Iterator<Item = &[u8]>,
            ocsp_response: &[u8],
            now: SystemTime,
        ) -> std::result::Result<ServerCertVerified, Error> {
            Ok(ServerCertVerified::assertion())
        }
    }

    #[test]
    #[allow(unused)]
    #[should_panic]
    fn test_rustls() {
        let dsn = env::var("EXA_DSN").unwrap();
        let schema = env::var("EXA_SCHEMA").unwrap();
        let user = env::var("EXA_USER").unwrap();
        let password = env::var("EXA_PASSWORD").unwrap();

        let mut opts = ConOpts::new();
        opts.set_login_kind(LoginKind::Credentials(Credentials::new(user, password)));
        opts.set_dsn(dsn);
        opts.set_schema(Some(schema));

        let mut config = ClientConfig::builder()
            .with_safe_defaults()
            .with_custom_certificate_verifier(Arc::new(CertVerifier))
            .with_no_client_auth();
        let connector = Connector::Rustls(Arc::new(config));

        let mut exa_con = Connection::from_connector(connector, opts).unwrap();
    }
}
