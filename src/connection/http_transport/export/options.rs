use std::{
    cmp,
    fmt::Debug,
    io::{Error as IoError, Result as IoResult},
    net::{IpAddr, SocketAddr},
    pin::Pin,
    task::{ready, Context, Poll},
};

use crate::{
    connection::{
        http_transport::{make_worker, RowSeparator},
        websocket::socket::ExaSocket,
    },
    ExaConnection, ExaDatabaseError,
};

use arrayvec::ArrayString;
use async_compression::futures::bufread::GzipDecoder;
use futures_core::{future::BoxFuture, Future};
use futures_io::{AsyncBufRead, AsyncRead, ErrorKind};
use futures_util::{
    future::{try_join, try_join_all},
    io::BufReader,
    pin_mut, AsyncBufReadExt, AsyncReadExt, FutureExt,
};
use pin_project::pin_project;
use sqlx_core::executor::Executor;
use sqlx_core::Error as SqlxError;

const CRLF_DELIMITER: &[u8; 2] = b"\r\n";
const TRANSPORT_BUFFER_SIZE: usize = 65536;

/// Export options
#[derive(Debug)]
pub struct ExportOptions<'a, F, Fut, T>
where
    F: FnMut(ExaExportReader) -> Fut,
    Fut: Future<Output = Result<T, SqlxError>>,
{
    num_readers: usize,
    source: QueryOrTable<'a>,
    comment: Option<&'a str>,
    encoding: Option<&'a str>,
    null: &'a str,
    row_separator: RowSeparator,
    column_separator: &'a str,
    column_delimiter: &'a str,
    future_maker: F,
}

impl<'a, F, Fut, T> ExportOptions<'a, F, Fut, T>
where
    F: FnMut(ExaExportReader) -> Fut,
    Fut: Future<Output = Result<T, SqlxError>>,
{
    pub(crate) fn new(source: QueryOrTable<'a>, future_maker: F) -> Self {
        Self {
            num_readers: 0,
            source,
            comment: None,
            encoding: None,
            null: "",
            row_separator: RowSeparator::CRLF,
            column_separator: ",",
            column_delimiter: "\"",
            future_maker,
        }
    }

    pub async fn execute(&mut self, con: &mut ExaConnection) -> Result<Vec<T>, SqlxError> {
        let ips = con.ws.get_hosts().await?;
        let port = con.ws.socket_addr().port();
        let is_encrypted = con.attributes().encryption_enabled;
        let is_compressed = con.attributes().compression_enabled;
        let (sockets, addrs) = self.start_jobs(ips, port).await?.into_iter().unzip();
        let query = self.make_query(addrs, is_encrypted, is_compressed);

        let (_, output) = try_join(
            con.execute(query.as_str()),
            Self::continue_jobs(sockets, &mut self.future_maker),
        )
        .await?;

        Ok(output)
    }

    /// Sets the number of reader jobs that will be started.
    /// If set to `0`, then as many as possible will be used (one per node).
    /// Providing a number bigger than the number of nodes is the same as providing `0`.
    pub fn num_readers(&mut self, num_readers: usize) -> &mut Self {
        self.num_readers = num_readers;
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

    fn make_query(
        &self,
        addrs: Vec<SocketAddr>,
        is_encrypted: bool,
        is_compressed: bool,
    ) -> String {
        let mut query = String::new();

        if let Some(com) = self.comment {
            query.push_str("/*");
            query.push_str(com);
            query.push_str("*/");
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
        self.append_filenames(&mut query, addrs, is_encrypted, is_compressed);

        if let Some(enc) = self.encoding {
            query.push_str("ENCODING = '");
            query.push_str(enc);
            query.push('\'');
        }

        query.push_str("NULL = '");
        query.push_str(self.null);
        query.push('\'');

        query.push_str("COLUMN SEPARATOR = '");
        query.push_str(self.column_separator);
        query.push('\'');

        query.push_str("COLUMN DELIMITER = '");
        query.push_str(self.column_delimiter);
        query.push('\'');

        query.push_str("WITH COLUMN NAMES");

        query
    }

    fn append_filenames(
        &self,
        query: &mut String,
        addrs: Vec<SocketAddr>,
        is_encrypted: bool,
        is_compressed: bool,
    ) {
        let prefix = match is_encrypted {
            false => "http",
            true => "https",
        };

        let ext = match is_compressed {
            false => "csv",
            true => "gz",
        };

        for (idx, addr) in addrs.into_iter().enumerate() {
            let filename = format!("AT '{}://{}' FILE '{:0>5}.{}'", prefix, addr, idx, ext);
            query.push_str(&filename);
        }
    }

    async fn start_jobs(
        &self,
        ips: Vec<IpAddr>,
        port: u16,
    ) -> Result<Vec<(ExaSocket, SocketAddr)>, SqlxError> {
        let num_jobs = match 0 < self.num_readers && self.num_readers < ips.len() {
            true => self.num_readers,
            false => ips.len(),
        };

        let mut futures = Vec::with_capacity(num_jobs);
        for ip in ips.into_iter().take(num_jobs) {
            futures.push(make_worker(ip, port))
        }

        try_join_all(futures)
            .await
            .map_err(|e| ExaDatabaseError::unknown(e.to_string()))
            .map_err(From::from)
    }

    async fn continue_jobs(
        sockets: Vec<ExaSocket>,
        future_maker: &mut F,
    ) -> Result<Vec<T>, SqlxError> {
        let futures_iter = sockets
            .into_iter()
            .map(|socket| Self::skip_headers(socket))
            .collect::<Vec<_>>();

        let exa_readers = try_join_all(futures_iter)
            .await
            .map_err(|e| ExaDatabaseError::unknown(e.to_string()))?;

        let futures_iter = exa_readers.into_iter().map(future_maker);

        try_join_all(futures_iter)
            .await
            .map_err(|e| ExaDatabaseError::unknown(e.to_string()))
            .map_err(From::from)
    }

    async fn skip_headers(socket: ExaSocket) -> Result<ExaExportReader, SqlxError> {
        let socket = BufReader::new(socket);
        let mut exa_reader = ExaExportReader {
            socket,
            compression: false,
        };

        let mut line = String::new();

        // Read and ignore HTTP Request headers
        while line != "\r\n" {
            line.clear();
            exa_reader.read_line(&mut line).await?;
        }

        Ok(exa_reader)
    }
}

#[derive(Clone, Copy, Debug)]
pub enum QueryOrTable<'a> {
    Query(&'a str),
    Table(&'a str),
}

#[pin_project]
#[derive(Debug)]
pub struct ExportReader {
    socket: BufReader<ExaSocket>,
    state: ChunkReadingState,
}

impl ExportReader {
    fn read_byte(socket: &mut BufReader<ExaSocket>, cx: &mut Context) -> Poll<IoResult<u8>> {
        let mut buffer = [0; 1];
        let n = ready!(Pin::new(socket).poll_read(cx, &mut buffer))?;
        if n != 1 {
            return Poll::Ready(Err(IoError::new(
                ErrorKind::InvalidInput,
                "cannot read chunk size",
            )));
        }
        Poll::Ready(Ok(buffer[0]))
    }
}

impl AsyncRead for ExportReader {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<futures_io::Result<usize>> {
        let this = self.project();
        let (state, output) = match this.state {
            ChunkReadingState::Size(s) => {
                let byte = ready!(Self::read_byte(this.socket, cx))?;
                let char = byte as char;
                s.push(char);

                let state = if s.ends_with("\r\n") {
                    let chunk_size =
                        usize::from_str_radix(s.as_str().trim_end(), 16).map_err(|_| {
                            IoError::new(ErrorKind::InvalidInput, "cannot read chunk size")
                        })?;

                    Some(ChunkReadingState::Data(chunk_size))
                } else {
                    None
                };

                (state, Poll::Pending)
            }
            ChunkReadingState::Data(remaining) => {
                if *remaining > 0 {
                    let num_bytes = ready!(Pin::new(this.socket).poll_read(cx, buf))?;
                    *remaining -= num_bytes;
                    (None, Poll::Ready(Ok(num_bytes)))
                } else {
                    (Some(ChunkReadingState::CR), Poll::Pending)
                }
            }
            ChunkReadingState::CR => {
                let byte = ready!(Self::read_byte(this.socket, cx))?;

                if byte != b'\r' {
                    Err(IoError::new(ErrorKind::InvalidInput, "problem"))?;
                }

                (Some(ChunkReadingState::LF), Poll::Pending)
            }
            ChunkReadingState::LF => {
                let byte = ready!(Self::read_byte(this.socket, cx))?;

                if byte != b'\n' {
                    Err(IoError::new(ErrorKind::InvalidInput, "problem"))?;
                }

                (
                    Some(ChunkReadingState::Size(ArrayString::new_const())),
                    Poll::Pending,
                )
            }
        };

        if let Some(state) = state {
            *this.state = state;
        }

        if output.is_pending() {
            cx.waker().wake_by_ref();
        }

        output
    }
}

#[pin_project]
#[derive(Debug)]
pub struct ExaExportReader {
    #[pin]
    socket: BufReader<ExaSocket>,
    compression: bool,
}

impl AsyncRead for ExaExportReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<IoResult<usize>> {
        self.project().socket.poll_read(cx, buf)
    }
}

impl AsyncBufRead for ExaExportReader {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<&[u8]>> {
        self.project().socket.poll_fill_buf(cx)
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        self.project().socket.consume(amt)
    }
}

#[derive(Debug)]
enum ChunkReadingState {
    Size(ArrayString<24>),
    Data(usize),
    CR,
    LF,
}
