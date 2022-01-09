//! Tests unencrypted compression support
//! included fingerprint in the DSN, but since we're not using encryption it will just
//! be discarded.

#[cfg(test)]
#[cfg(feature = "flate2")]
#[allow(unused)]
mod tests {
    use exasol::error::Result;
    use exasol::*;
    use std::env;

    #[test]
    #[allow(unused)]
    fn test_compression() {
        let dsn = env::var("EXA_DSN").unwrap();
        let schema = env::var("EXA_SCHEMA").unwrap();
        let user = env::var("EXA_USER").unwrap();
        let password = env::var("EXA_PASSWORD").unwrap();

        let mut opts = ConOpts::new();
        opts.set_login_kind(LoginKind::Credentials(Credentials::new(user, password)));
        opts.set_dsn(dsn);
        opts.set_compression(true);
        println!("Default encryption without flags: {}", opts.encryption());

        let mut exa_con = Connection::new(opts).unwrap();
        let mut result = exa_con
            .execute("SELECT * FROM RUST.EXA_RUST_TEST LIMIT 2001;")
            .unwrap();

        let rows: Vec<(String, String, u16)> = exa_con
            .iter_result(&mut result)
            .collect::<Result<_>>()
            .unwrap();

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
        http_opts.set_encryption(false);
        http_opts.set_compression(true);
        http_opts.set_query("SELECT * FROM RUST.EXA_RUST_TEST LIMIT 2001");
        exa_con.export_to_file("test_data.csv", http_opts).unwrap();

        let mut http_opts = ImportOpts::new();
        http_opts.set_encryption(false);
        http_opts.set_compression(true);
        http_opts.set_table_name("RUST.EXA_RUST_TEST");
        exa_con
            .import_from_file("test_data.csv", http_opts)
            .unwrap();
    }
}
