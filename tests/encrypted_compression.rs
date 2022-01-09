//! Tests encrypted compression support with native-tls and flate2 features
//! We'll also include a fingerprint in the DSN

#[cfg(test)]
#[cfg(all(feature = "native-tls-basic", feature = "flate2"))]
#[allow(unused)]
mod tests {
    use __native_tls::TlsConnector;
    use exasol::error::Result;
    use exasol::*;
    use std::env;
    use std::time::Duration;

    #[test]
    #[allow(unused)]
    fn test_enc_compression() {
        let dsn = env::var("EXA_DSN").unwrap();
        let schema = env::var("EXA_SCHEMA").unwrap();
        let user = env::var("EXA_USER").unwrap();
        let password = env::var("EXA_PASSWORD").unwrap();

        let mut opts = ConOpts::new();
        opts.set_login_kind(LoginKind::Credentials(Credentials::new(user, password)));
        opts.set_dsn(dsn);
        opts.set_schema(Some(schema));
        opts.set_compression(true);
        println!("Default encryption with flags: {}", opts.encryption());

        let mut config = TlsConnector::builder()
            .danger_accept_invalid_certs(true)
            .build()
            .unwrap();
        let connector = Connector::NativeTls(config);

        let mut exa_con = Connection::from_connector(connector, opts).unwrap();
        let mut result = exa_con
            .execute("SELECT * FROM RUST.EXA_RUST_TEST LIMIT 2001;")
            .unwrap();

        let mut http_opts = ExportOpts::new();
        http_opts.set_encryption(true);
        http_opts.set_compression(true);
        http_opts.set_query("SELECT * FROM RUST.EXA_RUST_TEST LIMIT 2001");
        let data: Vec<(String, String, u16)> = exa_con.export_to_vec(http_opts).unwrap();

        let mut http_opts = ImportOpts::new();
        http_opts.set_encryption(true);
        http_opts.set_compression(true);
        http_opts.set_table_name("RUST.EXA_RUST_TEST");
        exa_con.import_from_iter(data, http_opts).unwrap();

        let mut http_opts = ExportOpts::new();
        http_opts.set_encryption(false);
        http_opts.set_compression(true);
        http_opts.set_query("SELECT * FROM RUST.EXA_RUST_TEST LIMIT 2001");
        let data: Vec<(String, String, u16)> = exa_con.export_to_vec(http_opts).unwrap();

        let mut http_opts = ImportOpts::new();
        http_opts.set_encryption(false);
        http_opts.set_compression(true);
        http_opts.set_table_name("RUST.EXA_RUST_TEST");
        exa_con.import_from_iter(data, http_opts).unwrap();

        let mut http_opts = ExportOpts::new();
        http_opts.set_encryption(true);
        http_opts.set_compression(true);
        http_opts.set_query("SELECT * FROM RUST.EXA_RUST_TEST LIMIT 2001");
        exa_con.export_to_file("test_data.csv", http_opts).unwrap();

        let mut http_opts = ImportOpts::new();
        http_opts.set_encryption(true);
        http_opts.set_compression(true);
        http_opts.set_table_name("RUST.EXA_RUST_TEST");
        exa_con
            .import_from_file("test_data.csv", http_opts)
            .unwrap();
    }
}
