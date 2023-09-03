use std::{fmt::Debug, net::SocketAddrV4};

use sqlx_core::Error as SqlxError;

use super::ExaExport;
use crate::{
    connection::etl::RowSeparator,
    etl::{prepare, traits::EtlJob, JobFuture, SocketFuture},
    ExaConnection,
};

/// Export options
#[derive(Debug)]
pub struct ExportBuilder<'a> {
    num_readers: usize,
    compression: Option<bool>,
    source: QueryOrTable<'a>,
    comment: Option<&'a str>,
    encoding: Option<&'a str>,
    null: &'a str,
    row_separator: RowSeparator,
    column_separator: &'a str,
    column_delimiter: &'a str,
    with_column_names: bool,
}

impl<'a> ExportBuilder<'a> {
    pub fn new(source: QueryOrTable<'a>) -> Self {
        Self {
            num_readers: 0,
            compression: None,
            source,
            comment: None,
            encoding: None,
            null: "",
            row_separator: RowSeparator::CRLF,
            column_separator: ",",
            column_delimiter: "\"",
            with_column_names: false,
        }
    }

    /// Builds the EXPORT job.
    ///
    /// This implies submitting the EXPORT query.
    /// The output will be a future to await the result of the job
    /// and the workers that can be used for ETL.
    ///
    /// # Errors
    ///
    /// Returns an error if the job could not be built and submitted.
    pub async fn build<'c>(
        &'a self,
        con: &'c mut ExaConnection,
    ) -> Result<(JobFuture<'c>, Vec<ExaExport>), SqlxError>
    where
        'c: 'a,
    {
        prepare(self, con).await
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
        self.compression = Some(enabled);
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
}

impl<'a> EtlJob for ExportBuilder<'a> {
    const JOB_TYPE: &'static str = "export";

    type Worker = ExaExport;

    fn use_compression(&self) -> Option<bool> {
        self.compression
    }

    fn num_workers(&self) -> usize {
        self.num_readers
    }

    fn create_workers(
        &self,
        socket_futures: Vec<SocketFuture>,
        with_compression: bool,
    ) -> Vec<Self::Worker> {
        socket_futures
            .into_iter()
            .map(|f| ExaExport::Setup(f, with_compression))
            .collect()
    }

    fn query(&self, addrs: Vec<SocketAddrV4>, with_tls: bool, with_compression: bool) -> String {
        let mut query = String::new();

        if let Some(comment) = self.comment {
            Self::push_comment(&mut query, comment);
        }

        query.push_str("EXPORT ");

        match self.source {
            QueryOrTable::Table(tbl) => {
                Self::push_ident(&mut query, tbl);
            }
            QueryOrTable::Query(qr) => {
                query.push_str("(\n");
                query.push_str(qr);
                query.push_str("\n)");
            }
        };

        query.push(' ');

        query.push_str(" INTO CSV ");
        Self::append_files(&mut query, addrs, with_tls, with_compression);

        if let Some(enc) = self.encoding {
            Self::push_key_value(&mut query, "ENCODING", enc);
        }

        Self::push_key_value(&mut query, "NULL", self.null);
        Self::push_key_value(&mut query, "ROW SEPARATOR", self.row_separator.as_ref());
        Self::push_key_value(&mut query, "COLUMN SEPARATOR", self.column_separator);
        Self::push_key_value(&mut query, "COLUMN DELIMITER", self.column_delimiter);

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
