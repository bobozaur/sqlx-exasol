use std::{fmt::Debug, net::SocketAddr};

use crate::{
    connection::{
        http_transport::{append_filenames, start_jobs, RowSeparator},
        websocket::socket::ExaSocket,
    },
    ExaConnection, ExaQueryResult,
};

use futures_core::Future;

use sqlx_core::{error::BoxDynError, Error as SqlxError};

use super::{reader::ExportReader, ExaExport};

/// Export options
#[derive(Debug)]
pub struct ExportOptions<'a> {
    num_readers: usize,
    compression: bool,
    source: QueryOrTable<'a>,
    comment: Option<&'a str>,
    encoding: Option<&'a str>,
    null: &'a str,
    row_separator: RowSeparator,
    column_separator: &'a str,
    column_delimiter: &'a str,
    with_column_names: bool,
}

impl<'a> ExportOptions<'a> {
    pub fn new(source: QueryOrTable<'a>) -> Self {
        Self {
            num_readers: 0,
            compression: false,
            source,
            comment: None,
            encoding: None,
            null: "",
            row_separator: RowSeparator::CRLF,
            column_separator: ",",
            column_delimiter: "\"",
            with_column_names: true,
        }
    }

    pub async fn execute<'b>(
        &mut self,
        con: &'b mut ExaConnection,
    ) -> Result<
        (
            impl Future<Output = Result<ExaQueryResult, BoxDynError>> + 'b,
            Vec<ExaExport>,
        ),
        SqlxError,
    > {
        let ips = con.ws.get_hosts().await?;
        let port = con.ws.socket_addr().port();
        let encrypted = con.attributes().encryption_enabled;

        let (raw_sockets, addrs): (Vec<ExaSocket>, _) =
            start_jobs(self.num_readers, ips, port, encrypted)
                .await?
                .into_iter()
                .unzip();

        let query = self.query(addrs, encrypted, self.compression);

        let sockets = raw_sockets
            .into_iter()
            .map(ExportReader::new)
            .map(|r| ExaExport::new(r, self.compression))
            .collect();

        Ok((con.run_http_transport(query), sockets))
    }

    /// Sets the number of reader jobs that will be started.
    /// If set to `0`, then as many as possible will be used (one per node).
    /// Providing a number bigger than the number of nodes is the same as providing `0`.
    pub fn num_readers(&mut self, num_readers: usize) -> &mut Self {
        self.num_readers = num_readers;
        self
    }

    #[cfg(feature = "compression")]
    pub fn compression(&mut self, enabled: bool) -> &mut Self {
        self.compression = enabled;
        self
    }

    pub fn comment(&mut self, comment: &'a str) -> &mut Self {
        self.comment = Some(comment);
        self
    }

    pub fn encoding(&mut self, encoding: &'a str) -> &mut Self {
        self.encoding = Some(encoding);
        self
    }

    pub fn null(&mut self, null: &'a str) -> &mut Self {
        self.null = null;
        self
    }

    pub fn row_separator(&mut self, separator: RowSeparator) -> &mut Self {
        self.row_separator = separator;
        self
    }

    pub fn column_separator(&mut self, separator: &'a str) -> &mut Self {
        self.column_separator = separator;
        self
    }

    pub fn column_delimiter(&mut self, delimiter: &'a str) -> &mut Self {
        self.column_delimiter = delimiter;
        self
    }

    pub fn with_column_names(&mut self, flag: bool) -> &mut Self {
        self.with_column_names = flag;
        self
    }

    fn query(&self, addrs: Vec<SocketAddr>, is_encrypted: bool, is_compressed: bool) -> String {
        let mut query = String::new();

        if let Some(com) = self.comment {
            query.push_str("/*");
            query.push_str(com);
            query.push_str("*/\n");
        }

        query.push_str("EXPORT ");

        match self.source {
            QueryOrTable::Table(t) => query.push_str(t),
            QueryOrTable::Query(q) => {
                query.push_str("(\n");
                query.push_str(q);
                query.push_str("\n)");
            }
        };

        query.push_str(" INTO CSV ");

        append_filenames(&mut query, addrs, is_encrypted, is_compressed);

        if let Some(enc) = self.encoding {
            query.push_str(" ENCODING = '");
            query.push_str(enc);
            query.push('\'');
        }

        query.push_str(" NULL = '");
        query.push_str(self.null);
        query.push('\'');

        query.push_str(" COLUMN SEPARATOR = '");
        query.push_str(self.column_separator);
        query.push('\'');

        query.push_str(" COLUMN DELIMITER = '");
        query.push_str(self.column_delimiter);
        query.push('\'');

        if self.with_column_names {
            query.push_str(" WITH COLUMN NAMES");
        }

        query
    }
}

#[derive(Clone, Copy, Debug)]
pub enum QueryOrTable<'a> {
    Query(&'a str),
    Table(&'a str),
}
