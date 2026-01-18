use std::{
    fmt::Debug,
    io,
    net::SocketAddrV4,
    sync::{atomic::AtomicBool, Arc},
};

use flume::{Receiver, Sender};
use futures_util::task::AtomicWaker;
use sqlx_core::rt::JoinHandle;

use crate::{
    connection::etl::RowSeparator,
    etl::{
        export::{
            compression::MaybeCompressedReader, service::ExportService, ExaExport,
            ExportDataReceiver,
        },
        job::{EtlJob, SocketHandshake},
        server::OneShotHttpServer,
        EtlQuery,
    },
    ExaConnection, SqlxResult,
};

/// A builder for an ETL `EXPORT` job.
///
/// This builder allows configuring various options for the `EXPORT` job, such as the source,
/// number of readers, and CSV formatting options.
///
/// Once configured, the [`ExportBuilder::build`] method will create the `EXPORT` query and return
/// a tuple containing the [`EtlQuery`] future and a [`Vec<ExaReader>`] of workers. The future
/// must be awaited to drive the job to completion, while the workers are used to read the exported
/// data.
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
    pub fn new_from_query(query: &'a str) -> Self {
        Self::new(ExportSource::Query(query))
    }

    #[must_use]
    pub fn new_from_table(name: &'a str, schema: Option<&'a str>) -> Self {
        Self::new(ExportSource::Table { name, schema })
    }

    fn new(source: ExportSource<'a>) -> Self {
        Self {
            source,
            num_readers: 0,
            compression: None,
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
    /// The output will be a future to await the result of the `EXPORT` query and the workers that
    /// can be used for ETL IO.
    ///
    /// # Errors
    ///
    /// Returns an error if getting the nodes IPs from Exasol fails or if the worker sockets could
    /// not be connected.
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
    type DataPipe = ExportDataReceiver;

    fn use_compression(&self) -> Option<bool> {
        self.compression
    }

    fn num_workers(&self) -> usize {
        self.num_readers
    }

    fn create_worker(&self, rx: Receiver<Self::DataPipe>, with_compression: bool) -> Self::Worker {
        ExaExport(MaybeCompressedReader::new(rx, with_compression))
    }

    fn create_server_task(
        &self,
        tx: Sender<Self::DataPipe>,
        socket_future: SocketHandshake,
        waker: Arc<AtomicWaker>,
        stop: Arc<AtomicBool>,
    ) -> JoinHandle<io::Result<()>> {
        sqlx_core::rt::spawn(OneShotHttpServer::new(
            socket_future,
            ExportService::new(tx),
            waker,
            stop,
        ))
    }

    fn query(&self, addrs: Vec<SocketAddrV4>, with_tls: bool, with_compression: bool) -> String {
        let mut query = String::new();

        if let Some(comment) = self.comment {
            Self::push_comment(&mut query, comment);
        }

        query.push_str("EXPORT ");

        match self.source {
            ExportSource::Table { name, schema } => {
                if let Some(schema) = schema {
                    Self::push_ident(&mut query, schema);
                    query.push('.');
                }
                Self::push_ident(&mut query, name);
            }
            ExportSource::Query(qr) => {
                query.push_str("(\n");
                query.push_str(qr);
                query.push_str("\n)");
            }
        }

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

/// The EXPORT source type, which can either directly be a table or an entire query.
#[derive(Clone, Copy, Debug)]
enum ExportSource<'a> {
    Query(&'a str),
    Table {
        name: &'a str,
        schema: Option<&'a str>,
    },
}
