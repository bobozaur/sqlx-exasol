mod config;
mod reader;
mod stream;
mod worker;
mod writer;

use super::TRANSPORT_BUFFER_SIZE;
use crate::error::{DriverError, HttpTransportError, Result};
use crate::Connection;
use config::HttpTransportConfig;
pub use config::{ExportOpts, HttpTransportOpts, ImportOpts, TrimType};
use crossbeam::channel::{Receiver, Sender};
use crossbeam::thread::{Scope, ScopedJoinHandle};
use log::debug;
use rand::prelude::SliceRandom;
use rand::thread_rng;
pub use reader::ExaReader;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use worker::reader::ExaRowReader;
use worker::writer::ExaRowWriter;
use worker::{HttpExportThread, HttpImportThread, HttpTransportWorker};
pub use writer::ExaWriter;

/// Convenience alias
pub type TransportResult<T> = std::result::Result<T, HttpTransportError>;

/// Struct representing an EXPORT HTTP Transport job.
pub struct HttpExportJob<'a, T, F: FnOnce(ExaReader) -> T> {
    con: &'a mut Connection,
    opts: Option<ExportOpts>,
    callback: Option<F>,
}

impl<'a, T, F> HttpExportJob<'a, T, F>
where
    F: FnOnce(ExaReader) -> T,
{
    pub fn new(con: &'a mut Connection, callback: F, opts: ExportOpts) -> Self {
        Self {
            con,
            opts: Some(opts),
            callback: Some(callback),
        }
    }
}

impl<'a, T, F> HttpTransportJob for HttpExportJob<'a, T, F>
where
    F: FnOnce(ExaReader) -> T,
{
    type Opts = ExportOpts;
    type Worker = HttpExportThread;
    type DataHandler = Receiver<Vec<u8>>;
    type Callback = F;
    type Output = T;

    fn generate_channel() -> (
        <Self::Worker as HttpTransportWorker>::Channel,
        Self::DataHandler,
    ) {
        crossbeam::channel::unbounded()
    }

    fn generate_query(opts: Self::Opts, files: String) -> TransportResult<String> {
        let mut parts = Vec::new();

        if let Some(com) = opts.comment() {
            if com.contains("*/") {
                return Err(HttpTransportError::InvalidParameter(
                    "comment",
                    com.to_owned(),
                ));
            } else {
                parts.push(format!("/*{}*/", com));
            }
        }

        let source = if let Some(tbl) = opts.table_name() {
            tbl.to_owned()
        } else if let Some(q) = opts.query() {
            format!(
                "(\n{}\n)",
                q.trim_matches(|c| c == ' ' || c == '\n' || c == ';')
            )
        } else {
            return Err(HttpTransportError::InvalidParameter(
                "table_name or query",
                "".to_owned(),
            ));
        };

        parts.push(format!("EXPORT {} INTO CSV", source));
        parts.push(files);

        if let Some(enc) = opts.encoding() {
            parts.push(format!("ENCODING = '{}'", enc));
        }

        if let Some(n) = opts.null() {
            parts.push(format!("NULL = '{}'", n));
        }

        parts.push(format!(
            "COLUMN SEPARATOR = '{}'",
            opts.column_separator() as char
        ));

        parts.push(format!(
            "COLUMN DELIMITER = '{}'",
            opts.column_delimiter() as char
        ));

        if opts.with_column_names() {
            parts.push("WITH COLUMN NAMES".to_owned())
        }

        Ok(parts.join("\n"))
    }

    fn handle_data(handler: Self::DataHandler, callback: Self::Callback) -> Self::Output {
        let reader = ExaReader::new(handler);
        callback(reader)
    }

    fn get_opts(&mut self) -> Self::Opts {
        self.opts.take().unwrap_or_default()
    }

    fn get_parts(&mut self) -> (&mut Connection, Option<Self::Callback>) {
        (self.con, self.callback.take())
    }
}

/// Struct representing an IMPORT HTTP Transport job.
pub struct HttpImportJob<'a, T, F: FnOnce(ExaWriter) -> T> {
    con: &'a mut Connection,
    opts: Option<ImportOpts>,
    callback: Option<F>,
}

impl<'a, T, F> HttpImportJob<'a, T, F>
where
    F: FnOnce(ExaWriter) -> T,
{
    pub fn new(con: &'a mut Connection, callback: F, opts: ImportOpts) -> Self {
        Self {
            con,
            opts: Some(opts),
            callback: Some(callback),
        }
    }
}

impl<'a, T, F> HttpTransportJob for HttpImportJob<'a, T, F>
where
    F: FnOnce(ExaWriter) -> T,
{
    type Opts = ImportOpts;
    type Worker = HttpImportThread;
    type DataHandler = Sender<Vec<u8>>;
    type Callback = F;
    type Output = T;

    fn generate_channel() -> (
        <Self::Worker as HttpTransportWorker>::Channel,
        Self::DataHandler,
    ) {
        let (s, r) = crossbeam::channel::unbounded();
        (r, s)
    }

    fn generate_query(opts: Self::Opts, files: String) -> TransportResult<String> {
        let mut parts = Vec::new();

        if let Some(com) = opts.comment() {
            if com.contains("*/") {
                return Err(HttpTransportError::InvalidParameter(
                    "comment",
                    com.to_owned(),
                ));
            } else {
                parts.push(format!("/*{}*/", com));
            }
        }

        let source = if let Some(tbl) = opts.table_name() {
            tbl.to_owned()
        } else {
            return Err(HttpTransportError::InvalidParameter(
                "table_name or query",
                "".to_owned(),
            ));
        };

        let columns = if let Some(cols) = opts.columns() {
            format!("({})", cols.join(", "))
        } else {
            "".to_owned()
        };

        parts.push(format!("IMPORT INTO {} {} FROM CSV", source, columns));
        parts.push(files);

        if let Some(enc) = opts.encoding() {
            parts.push(format!("ENCODING = '{}'", enc));
        }

        if let Some(n) = opts.null() {
            parts.push(format!("NULL = '{}'", n));
        }

        if let Some(t) = opts.trim() {
            parts.push(format!("{}", t))
        }

        parts.push(format!("SKIP = {}", opts.skip()));

        parts.push(format!(
            "COLUMN SEPARATOR = '{}'",
            opts.column_separator() as char
        ));

        parts.push(format!(
            "COLUMN DELIMITER = '{}'",
            opts.column_delimiter() as char
        ));

        Ok(parts.join("\n"))
    }

    fn handle_data(handler: Self::DataHandler, callback: Self::Callback) -> Self::Output {
        let writer = ExaWriter::new(handler);
        callback(writer)
    }

    fn get_opts(&mut self) -> Self::Opts {
        self.opts.take().unwrap_or_default()
    }

    fn get_parts(&mut self) -> (&mut Connection, Option<Self::Callback>) {
        (self.con, self.callback.take())
    }
}

/// The heavy lifter of the HTTP Transport functionalities,
/// this trait defines the flow of HTTP transport, end-to-end.
pub trait HttpTransportJob {
    type Opts: HttpTransportOpts + Send;
    type Worker: HttpTransportWorker;
    type DataHandler: Clone + Send;
    type Callback;
    type Output;

    fn generate_channel() -> (
        <Self::Worker as HttpTransportWorker>::Channel,
        Self::DataHandler,
    );

    fn generate_query(opts: Self::Opts, files: String) -> TransportResult<String>;

    fn handle_data(handler: Self::DataHandler, callback: Self::Callback) -> Self::Output;

    /// Accessor for the underlying HTTP Transport options.
    fn get_opts(&mut self) -> Self::Opts;

    /// Accessor for the underlying fields.
    /// Retrieved together to satisfy the borrow checker.
    fn get_parts(&mut self) -> (&mut Connection, Option<Self::Callback>);

    fn run(&mut self) -> Result<Self::Output> {
        let scope = crossbeam::scope(|s| {
            let (worker_channel, data_handler) = Self::generate_channel();
            let opts = self.get_opts();
            let (con, cb) = self.get_parts();
            let (hosts, num_threads) = Self::get_node_addresses(con, &opts)?;

            debug!("Starting worker orchestrator...");
            // Start orchestrator thread
            let handle = s.spawn(move |s| {
                Self::orchestrate(s, worker_channel, hosts, num_threads, opts, con)
            });

            let output = cb
                .map(|callback| Self::handle_data(data_handler, callback))
                .ok_or(HttpTransportError::ThreadError)
                .map_err(DriverError::HttpTransportError)
                .map_err(crate::error::Error::DriverError);

            // We join on the orchestrator handle
            // to check for errors or get the data it retrieved.
            // Thread errors have higher priority than errors that occurred
            // in the query, because the query will generally error out if there
            // was a thread error, but the reverse is not true.
            let res = handle.join().map_err(|_| HttpTransportError::ThreadError);
            map_transport_result(res).and_then(|r| r).and(output)
        });

        let res = scope.map_err(|_| HttpTransportError::ThreadError);
        map_transport_result(res).and_then(|res| res)
    }

    /// Orchestrator that will spawn worker threads
    /// and execute the database query once all workers are connected.
    /// Returns the result generated by executing the query.
    fn orchestrate<'s, 'a>(
        s: &'s Scope<'a>,
        worker_channel: <Self::Worker as HttpTransportWorker>::Channel,
        hosts: Vec<String>,
        num_threads: usize,
        mut opts: Self::Opts,
        con: &'a mut Connection,
    ) -> Result<()>
    where
        <Self::Worker as HttpTransportWorker>::Channel: 'a,
    {
        // Setup worker configs
        let run = Arc::new(AtomicBool::new(true));
        let barrier = Arc::new(Barrier::new(num_threads + 1));
        let (addr_sender, addr_receiver) = crossbeam::channel::bounded(num_threads);
        let enc = opts.encryption();
        let cmp = opts.compression();
        let timeout = opts.take_timeout();
        let configs = HttpTransportConfig::generate(
            hosts,
            barrier.clone(),
            run.clone(),
            addr_sender,
            enc,
            cmp,
            timeout,
        );

        // Start worker threads
        let handles = Self::start_workers(s, configs, num_threads, worker_channel);

        debug!("Orchestrator waiting for internal Exasol addresses...");
        // Generate makeshift IMPORT/EXPORT filenames and locations
        let files = Self::generate_filenames(enc, cmp, num_threads, &addr_receiver);

        // Signal threads to stop if there was an error
        // on the orchestrator thread side
        if files.is_err() {
            run.store(false, Ordering::Release);
        }

        // Aligning threads to the moment that all workers are
        // connected and the query is ready for execution.
        barrier.wait();

        debug!("Orchestrator received addresses! Running query...");
        // Executing the query blocks until the transport is over
        let query_res = files.and_then(|hosts| {
            let query = Self::generate_query(opts, hosts);
            let query = query.map_err(DriverError::HttpTransportError)?;
            con.execute(query)?;
            Ok(())
        });

        // Synchronize worker threads with the orchestrator
        // as the sockets must not close before the server
        // reads everything from them (query execution finishes)
        barrier.wait();

        // Signal threads to stop if there was an error
        // on the orchestrator thread side
        if query_res.is_err() {
            run.store(false, Ordering::Release);
        }

        Self::join_handles(handles).and(query_res)
    }

    /// Joins on all worker handles
    fn join_handles(handles: Vec<ScopedJoinHandle<TransportResult<()>>>) -> Result<()> {
        let res = handles
            .into_iter()
            .map(Self::join_handle)
            .fold(Ok(()), |_, r| r);
        map_transport_result(res)
    }

    /// Joins on one worker handle
    fn join_handle(h: ScopedJoinHandle<TransportResult<()>>) -> TransportResult<()> {
        h.join()
            .map_err(|_| HttpTransportError::ThreadError)
            .and_then(|r| r)
    }

    /// Start a worker thread for each config
    /// and return their handles
    fn start_workers<'s, 'a>(
        s: &'s Scope<'a>,
        configs: Vec<HttpTransportConfig>,
        num_threads: usize,
        worker_channel: <Self::Worker as HttpTransportWorker>::Channel,
    ) -> Vec<ScopedJoinHandle<'s, TransportResult<()>>>
    where
        <Self::Worker as HttpTransportWorker>::Channel: 'a,
    {
        debug!("Orchestrator will start {} workers", num_threads);
        configs
            .into_iter()
            .fold(Vec::with_capacity(num_threads), |mut handles, config| {
                let worker_channel = worker_channel.clone();

                handles.push(s.spawn(move |_| {
                    let mut het = Self::Worker::new(worker_channel);
                    het.start(config)
                }));

                handles
            })
    }

    /// Gather Exasol cluster node hosts for HTTP Transport
    fn get_node_addresses(con: &mut Connection, opts: &Self::Opts) -> Result<(Vec<String>, usize)> {
        // Get cluster hosts
        let mut hosts = con.get_nodes()?;

        // Calculate number of threads
        // The default is 0, which means a thread per cluster
        // will be used.
        // If a different value smaller than the number of nodes
        // in the cluster is provided, then that will be used.
        let hosts_len = hosts.len();
        let opts_len = opts.num_threads();
        let num_threads = match 0 < opts_len && opts_len < hosts_len {
            true => opts_len,
            false => hosts_len,
        };

        // Limit hosts to the number of threads we will spawn.
        // Randomizing them to balance load.
        hosts.shuffle(&mut thread_rng());
        let hosts = hosts.into_iter().take(num_threads).collect();
        Ok((hosts, num_threads))
    }

    /// Retrieves the internal addresses generated by
    /// each worker socket connection.
    /// These will be used in generating the filename part of the query.
    fn generate_filenames(
        use_encryption: bool,
        use_compression: bool,
        num_threads: usize,
        addr_receiver: &Receiver<String>,
    ) -> Result<String> {
        let prefix = match use_encryption {
            false => "http",
            true => "https",
        };

        let ext = match use_compression {
            false => "csv",
            true => "gz",
        };

        let res = (0..num_threads)
            .into_iter()
            .map(|i| Self::recv_address(prefix, ext, addr_receiver, i))
            .collect::<TransportResult<Vec<String>>>()
            .map(|v| v.join("\n"));
        map_transport_result(res)
    }

    /// Wrapper that receives and parses an Exasol internal address
    #[inline]
    fn recv_address(
        prefix: &str,
        ext: &str,
        receiver: &Receiver<String>,
        index: usize,
    ) -> TransportResult<String> {
        let addr = receiver
            .recv()
            .map(|s| format!("AT '{}://{}' FILE '{:0>3}.{}'", prefix, s, index, ext))?;
        Ok(addr)
    }
}

/// Convenience function for mapping a transport result to the crate's main result type
#[inline]
fn map_transport_result<T>(res: TransportResult<T>) -> Result<T> {
    res.map_err(DriverError::HttpTransportError)
        .map_err(From::from)
}
