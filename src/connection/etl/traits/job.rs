use crate::etl::SocketFuture;
use std::net::SocketAddrV4;

pub trait EtlJob: Send + Sync {
    const GZ_FILE_EXT: &'static str = "gz";
    const CSV_FILE_EXT: &'static str = "csv";

    const HTTP_SCHEME: &'static str = "http";
    const HTTPS_SCHEME: &'static str = "https";

    const JOB_TYPE: &'static str;

    type Worker;

    fn use_compression(&self) -> Option<bool>;

    fn num_workers(&self) -> usize;

    fn create_workers(
        &self,
        socket_futures: Vec<SocketFuture>,
        with_compression: bool,
    ) -> Vec<Self::Worker>;

    fn query(&self, addrs: Vec<SocketAddrV4>, with_tls: bool, with_compression: bool) -> String;

    fn append_files(
        query: &mut String,
        addrs: Vec<SocketAddrV4>,
        with_tls: bool,
        with_compression: bool,
    ) {
        let prefix = match with_tls {
            false => Self::HTTP_SCHEME,
            true => Self::HTTPS_SCHEME,
        };

        let ext = match with_compression {
            false => Self::CSV_FILE_EXT,
            true => Self::GZ_FILE_EXT,
        };

        for (idx, addr) in addrs.into_iter().enumerate() {
            let filename = format!(
                "AT '{}://{}' FILE '{}_{:0>5}.{}'\n",
                prefix,
                addr,
                Self::JOB_TYPE,
                idx,
                ext
            );
            query.push_str(&filename);
        }
    }

    fn push_comment(query: &mut String, comment: &str) {
        query.push_str("/*\n");
        query.push_str(comment);
        query.push_str("*/\n");
    }

    fn push_ident(query: &mut String, ident: &str) {
        query.push('"');
        query.push_str(ident);
        query.push('"');
    }

    fn push_literal(query: &mut String, lit: &str) {
        query.push('\'');
        query.push_str(lit);
        query.push_str("' ");
    }

    fn push_key_value(query: &mut String, key: &str, value: &str) {
        query.push_str(key);
        query.push_str(" = ");
        Self::push_literal(query, value);
    }
}
