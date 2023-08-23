use std::net::SocketAddr;

use futures_core::Future;
use sqlx_core::{error::BoxDynError, Error as SqlxError};

use crate::{
    connection::{
        http_transport::{append_filenames, start_jobs, RowSeparator},
        websocket::socket::ExaSocket,
    },
    ExaConnection, ExaQueryResult,
};

use super::{writer::ImportWriter, ExaImport};

#[derive(Clone, Debug)]
pub struct ImportOptions<'a, T = &'a str>
where
    T: AsRef<str>,
{
    num_writers: usize,
    buffer_size: usize,
    compression: bool,
    dest_table: &'a str,
    columns: Option<&'a [T]>,
    comment: Option<&'a str>,
    encoding: Option<&'a str>,
    null: &'a str,
    row_separator: RowSeparator,
    column_separator: &'a str,
    column_delimiter: &'a str,
    skip: u64,
    trim: Option<Trim>,
}

impl<'a> ImportOptions<'a> {
    pub fn new(dest_table: &'a str) -> Self {
        Self {
            num_writers: 0,
            buffer_size: Self::DEFAULT_BUF_SIZE,
            compression: false,
            dest_table,
            columns: None,
            comment: None,
            encoding: None,
            null: "",
            row_separator: RowSeparator::CRLF,
            column_separator: ",",
            column_delimiter: "\"",
            skip: 0,
            trim: None,
        }
    }
}

impl<'a, T> ImportOptions<'a, T>
where
    T: AsRef<str>,
{
    const DEFAULT_BUF_SIZE: usize = 65536;

    pub async fn execute<'b>(
        &mut self,
        con: &'b mut ExaConnection,
    ) -> Result<
        (
            impl Future<Output = Result<ExaQueryResult, BoxDynError>> + 'b,
            Vec<ExaImport>,
        ),
        SqlxError,
    > {
        let ips = con.ws.get_hosts().await?;
        let port = con.ws.socket_addr().port();
        let encrypted = con.attributes().encryption_enabled;

        let (raw_sockets, addrs): (Vec<ExaSocket>, _) =
            start_jobs(self.num_writers, ips, port, encrypted)
                .await?
                .into_iter()
                .unzip();

        let query = self.query(addrs, encrypted, self.compression);

        let sockets = raw_sockets
            .into_iter()
            .map(|s| ImportWriter::new(s, self.buffer_size))
            .map(|w| ExaImport::new(w, self.compression))
            .collect();

        Ok((con.run_http_transport(query), sockets))
    }

    /// Sets the number of writer jobs that will be started.
    /// If set to `0`, then as many as possible will be used (one per node).
    /// Providing a number bigger than the number of nodes is the same as providing `0`.
    pub fn num_writers(&mut self, num_writers: usize) -> &mut Self {
        self.num_writers = num_writers;
        self
    }

    pub fn buffer_size(&mut self, buffer_size: usize) -> &mut Self {
        self.buffer_size = buffer_size;
        self
    }

    #[cfg(feature = "compression")]
    pub fn compression(&mut self, enabled: bool) -> &mut Self {
        self.compression = enabled;
        self
    }

    pub fn columns(&mut self, columns: Option<&'a [T]>) -> &mut Self {
        self.columns = columns;
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

    pub fn skip(&mut self, num: u64) -> &mut Self {
        self.skip = num;
        self
    }

    pub fn trim(&mut self, trim: Trim) -> &mut Self {
        self.trim = Some(trim);
        self
    }

    fn query(&self, addrs: Vec<SocketAddr>, is_encrypted: bool, is_compressed: bool) -> String {
        let mut query = String::new();

        if let Some(com) = self.comment {
            query.push_str("/*");
            query.push_str(com);
            query.push_str("*/\n");
        }

        query.push_str("IMPORT INTO ");
        query.push_str(self.dest_table);
        query.push(' ');

        if let Some(cols) = self.columns {
            query.push('(');
            for col in cols.iter() {
                query.push('"');
                query.push_str(col.as_ref());
                query.push('"');
                query.push(',');
            }

            // Remove trailing comma
            query.pop();
            query.push_str(") ");
        }

        query.push_str("FROM CSV ");
        append_filenames(&mut query, addrs, is_encrypted, is_compressed);

        if let Some(enc) = self.encoding {
            query.push_str(" ENCODING = '");
            query.push_str(enc);
            query.push('\'');
        }

        query.push_str(" NULL = '");
        query.push_str(self.null);
        query.push('\'');

        if let Some(trim) = self.trim {
            query.push(' ');
            query.push_str(trim.as_ref());
        }

        query.push_str(" SKIP = ");
        query.push_str(&self.skip.to_string());

        query.push_str(" COLUMN SEPARATOR = '");
        query.push_str(self.column_separator);
        query.push('\'');

        query.push_str(" COLUMN DELIMITER = '");
        query.push_str(self.column_delimiter);
        query.push('\'');

        query.push_str(" ROW SEPARATOR = '");
        query.push_str(self.row_separator.as_ref());
        query.push('\'');

        query
    }
}

/// Trim options for IMPORT
#[derive(Debug, Clone, Copy)]
pub enum Trim {
    Left,
    Right,
    Both,
}

impl AsRef<str> for Trim {
    fn as_ref(&self) -> &str {
        match self {
            Self::Left => "LTRIM",
            Self::Right => "RTRIM",
            Self::Both => "TRIM",
        }
    }
}
