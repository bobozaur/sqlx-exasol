use super::stream::MaybeTlsStream;
use super::TRANSPORT_BUFFER_SIZE;
use super::{ExaRowReader, ExaRowWriter, HttpTransportConfig, TransportResult};
use crate::error::HttpTransportError;
use crossbeam::channel::{Receiver, Sender};
use log::debug;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

pub mod reader;
pub mod writer;

/// Special Exasol packet that enables tunneling.
/// Exasol responds with an internal address that can be used in query.
const SPECIAL_PACKET: [u8; 12] = [2, 33, 33, 2, 1, 0, 0, 0, 1, 0, 0, 0];

/// Packet that ends tunnelling
const END_PACKET: &[u8; 5] = b"0\r\n\r\n";

/// Packet that tells Exasol the transport was successful
const SUCCESS_HEADERS: &[u8; 66] = b"HTTP/1.1 200 OK\r\n\
                                     Connection: close\r\n\
                                     Transfer-Encoding: chunked\r\n\
                                     \r\n";

/// Packet that tells Exasol the transport had an error
const ERROR_HEADERS: &[u8; 57] = b"HTTP/1.1 500 Internal Server Error\r\n\
                                   Connection: close\r\n\
                                   \r\n";

/// HTTP Transport export worker thread.
pub struct HttpExportThread {
    data_handler: Sender<Vec<u8>>,
}

impl HttpTransportWorker for HttpExportThread {
    type Channel = Sender<Vec<u8>>;

    fn new(data_handler: Self::Channel) -> Self {
        Self { data_handler }
    }

    fn process_data(
        &mut self,
        stream: &mut MaybeTlsStream,
        run: &Arc<AtomicBool>,
        compression: bool,
    ) -> TransportResult<()> {
        let mut reader = BufReader::new(stream);
        Self::skip_headers(&mut reader, run)?;

        // Read all data in buffer.
        let mut processor = ExaRowReader::new(reader, compression);
        let mut num_bytes = 1;

        while num_bytes > 0 && run.load(Ordering::Acquire) {
            let mut buf = Vec::with_capacity(TRANSPORT_BUFFER_SIZE);
            num_bytes = processor.read_chunk(&mut buf)?;
            self.data_handler
                .send(buf)
                .map_err(|_| HttpTransportError::SendError)?;
        }

        Ok(())
    }

    fn success(stream: &mut MaybeTlsStream) -> TransportResult<()> {
        stream
            .write_all(SUCCESS_HEADERS)
            .and(stream.write_all(END_PACKET))
            .map_err(HttpTransportError::IoError)
    }

    // When doing an EXPORT an error also has to be signaled
    // to Exasol by sending the error headers.
    fn error(
        stream: &mut MaybeTlsStream,
        error: HttpTransportError,
        run: &Arc<AtomicBool>,
    ) -> TransportResult<()> {
        Self::stop(run);
        stream.write_all(ERROR_HEADERS).ok();
        Err(error)
    }
}

/// HTTP Transport import worker thread.
pub struct HttpImportThread {
    data_handler: Receiver<Vec<u8>>,
}

impl HttpTransportWorker for HttpImportThread {
    type Channel = Receiver<Vec<u8>>;
    fn new(data_handler: Self::Channel) -> Self {
        Self { data_handler }
    }

    fn process_data(
        &mut self,
        stream: &mut MaybeTlsStream,
        run: &Arc<AtomicBool>,
        compression: bool,
    ) -> TransportResult<()> {
        let mut reader = BufReader::new(stream);
        Self::skip_headers(&mut reader, run)?;

        let stream = reader.into_inner();
        stream.write_all(SUCCESS_HEADERS)?;

        let mut processor = ExaRowWriter::new(stream, compression);

        for chunk in &self.data_handler {
            processor.write_chunk(chunk)?
        }

        Ok(())
    }

    fn success(stream: &mut MaybeTlsStream) -> TransportResult<()> {
        stream
            .write_all(END_PACKET)
            .map_err(HttpTransportError::IoError)
    }

    // Errors will come from Exasol
    // So simply dropping the socket connection later will suffice.
    // No special action is required.
    fn error(
        _stream: &mut MaybeTlsStream,
        error: HttpTransportError,
        run: &Arc<AtomicBool>,
    ) -> TransportResult<()> {
        Self::stop(run);
        Err(error)
    }
}

/// Trait for Exasol HTTP Transport protocol implementation.
/// This trait provides the interface for a single worker to perform
/// operations related to an IMPORT or EXPORT job.
pub trait HttpTransportWorker {
    /// Communication channel type.
    type Channel: Clone + Send;

    /// Method to generate a new worker instance
    fn new(channel: Self::Channel) -> Self;

    /// Method to overwrite to IMPORT/EXPORT data.
    fn process_data(
        &mut self,
        stream: &mut MaybeTlsStream,
        run: &Arc<AtomicBool>,
        compression: bool,
    ) -> TransportResult<()>;

    /// Defines success behaviour
    fn success(stream: &mut MaybeTlsStream) -> TransportResult<()>;

    /// Defines error behaviour
    fn error(
        stream: &mut MaybeTlsStream,
        error: HttpTransportError,
        run: &Arc<AtomicBool>,
    ) -> TransportResult<()>;

    /// Signals to stop running the whole HTTP transport if an error was encountered.
    fn stop(run: &Arc<AtomicBool>) {
        run.store(false, Ordering::Release);
    }

    /// Starts HTTP Transport
    fn start(&mut self, mut config: HttpTransportConfig) -> TransportResult<()> {
        debug!("Worker starting...");
        // Initialize stream and send internal Exasol addresses to parent thread
        let timeout = config.take_timeout();
        let socket = Self::initialize(
            config.server_addr.as_str(),
            &mut config.addr_sender,
            timeout,
        );

        // Wait for the parent thread to read all addresses, compose and execute query
        config.barrier.wait();

        // Do actual data processing.
        let socket = socket
            .and_then(|stream| Self::promote(stream, config.encryption))
            .and_then(|stream| self.transport(stream, config.run, config.compression));

        // Wait for query execution to end before dropping the socket.
        config.barrier.wait();
        socket.map(|_| ())
    }

    /// Connects a [TcpStream] to the Exasol server,
    /// gets an internal Exasol address for HTTP transport,
    /// sends the address back to the parent thread
    /// and returns the [TcpStream] for further use.
    fn initialize<A>(
        server_addr: A,
        addr_sender: &mut Sender<String>,
        timeout: Option<Duration>,
    ) -> TransportResult<TcpStream>
    where
        A: ToSocketAddrs,
    {
        // Connects stream and writes special packet to retrieve
        // the internal Exasol address to be used in the query.
        // This must always be done unencrypted.
        let mut stream = TcpStream::connect(server_addr)?;
        stream.set_read_timeout(timeout)?;
        stream.set_write_timeout(timeout)?;
        stream.write_all(&SPECIAL_PACKET)?;
        stream.flush()?;

        // Read response buffer.
        let mut buf = [0; 24];
        stream.read_exact(&mut buf)?;

        // Parse response and sends address over to parent thread
        // to generate and execute the query.
        addr_sender
            .send(Self::parse_address(buf)?)
            .map_err(|_| HttpTransportError::SendError)?;

        // Return created stream
        Ok(stream)
    }

    /// Performs the actual data transport.
    /// We return the socket to avoid dropping it yet.
    fn transport(
        &mut self,
        mut stream: MaybeTlsStream,
        run: Arc<AtomicBool>,
        compression: bool,
    ) -> TransportResult<MaybeTlsStream> {
        let res = match run.load(Ordering::Acquire) {
            false => Err(HttpTransportError::ThreadError),
            true => self.process_data(&mut stream, &run, compression),
        };

        match res {
            Ok(_) => Self::success(&mut stream),
            Err(e) => Self::error(&mut stream, e, &run),
        }?;

        stream.flush()?;
        Ok(stream)
    }

    /// We don't do anything with the HTTP headers
    /// from Exasol, so we'll just read and discard them.
    fn skip_headers<R>(
        mut reader: R,
        run: &Arc<AtomicBool>,
    ) -> std::result::Result<(), std::io::Error>
    where
        R: BufRead,
    {
        let mut line = String::new();
        while run.load(Ordering::Acquire) && line != "\r\n" {
            line.clear();
            reader.read_line(&mut line)?;
        }
        Ok(())
    }

    /// Promotes stream to TLS and adds compression as needed
    fn promote(stream: TcpStream, encryption: bool) -> TransportResult<MaybeTlsStream> {
        MaybeTlsStream::wrap(stream, encryption)
    }

    /// Parses response to return the internal Exasol address
    /// to be used in query.
    fn parse_address(buf: [u8; 24]) -> TransportResult<String> {
        let port_bytes = <[u8; 4]>::try_from(&buf[4..8])?;
        let port = u32::from_le_bytes(port_bytes);

        let mut ipaddr = String::with_capacity(16);
        buf[8..]
            .iter()
            .take_while(|b| **b != b'\0')
            .for_each(|b| ipaddr.push(char::from(*b)));

        Ok(format!("{}:{}", ipaddr, port))
    }
}
