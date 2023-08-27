use std::{fmt::Write, net::SocketAddrV4};

use arrayvec::ArrayString;
use sqlx_core::Error as SqlxError;

use crate::{
    connection::{etl::RowSeparator, websocket::socket::ExaSocket},
    etl::{prepare, traits::EtlJob, JobFuture},
    ExaConnection,
};

use super::{writer::ImportWriter, ExaImport};

#[derive(Clone, Debug)]
pub struct ImportBuilder<'a> {
    num_writers: usize,
    buffer_size: usize,
    compression: Option<bool>,
    dest_table: &'a str,
    columns: Option<&'a [&'a str]>,
    comment: Option<&'a str>,
    encoding: Option<&'a str>,
    null: &'a str,
    row_separator: RowSeparator,
    column_separator: &'a str,
    column_delimiter: &'a str,
    skip: u64,
    trim: Option<Trim>,
}

impl<'a> ImportBuilder<'a> {
    const DEFAULT_BUF_SIZE: usize = 65536;

    pub fn new(dest_table: &'a str) -> Self {
        Self {
            num_writers: 0,
            buffer_size: Self::DEFAULT_BUF_SIZE,
            compression: None,
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

    pub async fn build<'c>(
        &'a self,
        con: &'c mut ExaConnection,
    ) -> Result<(JobFuture<'c>, Vec<ExaImport>), SqlxError>
    where
        'c: 'a,
    {
        prepare(self, con).await
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
        self.compression = Some(enabled);
        self
    }

    pub fn columns(&mut self, columns: Option<&'a [&'a str]>) -> &mut Self {
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
}

impl<'a> EtlJob for ImportBuilder<'a> {
    const JOB_TYPE: &'static str = "import";

    type Worker = ExaImport;

    fn use_compression(&self) -> Option<bool> {
        self.compression
    }

    fn num_workers(&self) -> usize {
        self.num_writers
    }

    fn create_workers(&self, sockets: Vec<ExaSocket>, with_compression: bool) -> Vec<Self::Worker> {
        sockets
            .into_iter()
            .map(|s| ImportWriter::new(s, self.buffer_size))
            .map(|w| ExaImport::new(w, with_compression))
            .collect()
    }

    fn query(&self, addrs: Vec<SocketAddrV4>, with_tls: bool, with_compression: bool) -> String {
        let mut query = String::new();

        if let Some(comment) = self.comment {
            Self::push_comment(&mut query, comment);
        }

        query.push_str("IMPORT INTO ");
        Self::push_ident(&mut query, self.dest_table);
        query.push(' ');

        // Push comma separated IMPORT columns
        if let Some(cols) = self.columns {
            query.push('(');
            for col in cols.iter() {
                query.push_str(col);
                query.push_str(", ");
            }

            // Remove trailing comma and space
            query.pop();
            query.pop();
            query.push_str(") ");
        }

        query.push_str("FROM CSV ");
        Self::append_files(&mut query, addrs, with_tls, with_compression);

        if let Some(enc) = self.encoding {
            Self::push_key_value(&mut query, "ENCODING", enc);
        }

        Self::push_key_value(&mut query, "NULL", self.null);
        Self::push_key_value(&mut query, "ROW SEPARATOR", self.row_separator.as_ref());
        Self::push_key_value(&mut query, "COLUMN SEPARATOR", self.column_separator);
        Self::push_key_value(&mut query, "COLUMN DELIMITER", self.column_delimiter);

        let mut skip_str = ArrayString::<20>::new_const();
        write!(&mut skip_str, "{}", self.skip).expect("u64 can't have more than 20 digits");

        // This is numeric, so no quoting
        query.push_str("SKIP = ");
        query.push_str(&skip_str);
        query.push(' ');

        if let Some(trim) = self.trim {
            query.push_str(trim.as_ref());
        }

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
