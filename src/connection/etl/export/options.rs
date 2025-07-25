use std::{fmt::Debug, net::SocketAddrV4};

use super::{ExaExport, ExportSource};
use crate::{
    connection::etl::RowSeparator,
    etl::{
        export::ExaExportState,
        job::{EtlJob, SocketSetup},
        EtlQuery,
    },
    ExaConnection, SqlxResult,
};

/// A builder for an ETL EXPORT job.
///
/// Calling [`build().await`] will ouput a future that drives the EXPORT query execution and a
/// [`Vec<ExaReader>`] which must be concurrently used to read data from Exasol.
#[derive(Debug)]
pub struct ExportBuilder<'a> {
    num_readers: usize,
    compression: Option<bool>,
    source: ExportSource<'a>,
    comment: Option<&'a str>,
    encoding: Option<&'a str>,
    null: &'a str,
    row_separator: RowSeparator,
    column_separator: &'a str,
    column_delimiter: &'a str,
    with_column_names: bool,
}

impl<'a> ExportBuilder<'a> {
    #[must_use]
    pub fn new(source: ExportSource<'a>) -> Self {
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
    /// This implies submitting the EXPORT query. The output will be a future to await the result of
    /// the job and the workers that can be used for ETL IO.
    ///
    /// # Errors
    ///
    /// Returns an error if the job could not be built and submitted.
    pub async fn build<'c>(
        &'a self,
        con: &'c mut ExaConnection,
    ) -> SqlxResult<(EtlQuery<'c>, Vec<ExaExport>)>
    where
        'c: 'a,
    {
        self.build_job(con).await
    }

    /// Sets the number of reader jobs that will be started.
    ///
    /// If set to `0`, then as many as possible will be used (one per node). Providing a number
    /// bigger than the number of nodes is the same as providing `0`.
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

impl EtlJob for ExportBuilder<'_> {
    const JOB_TYPE: &'static str = "export";

    type Worker = ExaExport;

    fn use_compression(&self) -> Option<bool> {
        self.compression
    }

    fn num_workers(&self) -> usize {
        self.num_readers
    }

    fn create_worker(&self, setup_future: SocketSetup, with_compression: bool) -> Self::Worker {
        ExaExport(ExaExportState::Setup(setup_future, with_compression))
    }

    fn query(&self, addrs: Vec<SocketAddrV4>, with_tls: bool, with_compression: bool) -> String {
        let mut query = String::new();

        if let Some(comment) = self.comment {
            Self::push_comment(&mut query, comment);
        }

        query.push_str("EXPORT ");

        match self.source {
            ExportSource::Table(tbl) => {
                Self::push_ident(&mut query, tbl);
            }
            ExportSource::Query(qr) => {
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
