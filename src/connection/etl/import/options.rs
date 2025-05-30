use std::{fmt::Write, net::SocketAddrV4};

use arrayvec::ArrayString;
use sqlx_core::Error as SqlxError;

use super::{ExaImport, Trim};
use crate::{
    connection::etl::RowSeparator,
    etl::{build_etl, traits::EtlJob, EtlQuery, SocketFuture},
    ExaConnection,
};

/// A builder for an ETL IMPORT job.
/// Calling [`build().await`] will ouput a future that drives the IMPORT query execution and a
/// [`Vec<ExaWriter>`] which must be concurrently used to ingest data into Exasol.
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
    #[must_use]
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

    /// Builds the IMPORT job.
    ///
    /// This implies submitting the IMPORT query.
    /// The output will be a future to await the result of the job
    /// and the workers that can be used for ETL IO.
    ///
    /// # Errors
    ///
    /// Returns an error if the job could not be built and submitted.
    pub async fn build<'c>(
        &'a self,
        con: &'c mut ExaConnection,
    ) -> Result<(EtlQuery<'c>, Vec<ExaImport>), SqlxError>
    where
        'c: 'a,
    {
        build_etl(self, con).await
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

    fn create_workers(
        &self,
        socket_futures: Vec<SocketFuture>,
        with_compression: bool,
    ) -> Vec<Self::Worker> {
        socket_futures
            .into_iter()
            .map(|f| ExaImport::Setup(f, self.buffer_size, with_compression))
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
            for col in cols {
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
