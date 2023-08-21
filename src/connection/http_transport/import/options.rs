use arrayvec::ArrayString;
use futures_core::Future;
use futures_io::AsyncWrite;
use futures_util::{future::try_join, io::BufWriter, AsyncWriteExt};
use pin_project::pin_project;
use sqlx_core::Error as SqlxError;

use std::{
    cmp,
    io::{Error as IoError, ErrorKind as IoErrorKind, Result as IoResult, Write},
    pin::Pin,
    task::{ready, Context, Poll},
};

use crate::{
    connection::{http_transport::RowSeparator, websocket::socket::ExaSocket},
    ExaConnection,
};

#[derive(Clone, Debug)]
pub struct ImportOptions<'a> {
    num_writers: usize,
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

impl<'a> ImportOptions<'a> {
    pub(crate) fn new(dest_table: &'a str, columns: Option<&'a [&'a str]>) -> Self {
        Self {
            num_writers: 0,
            dest_table,
            columns,
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

    /// Sets the number of writer jobs that will be started.
    /// If set to `0`, then as many as possible will be used (one per node).
    /// Providing a number bigger than the number of nodes is the same as providing `0`.
    pub fn num_writers(&mut self, num_writers: usize) -> &mut Self {
        self.num_writers = num_writers;
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

#[pin_project]
#[derive(Debug)]
pub struct ImportWriter {
    #[pin]
    socket: ExaSocket,
    buf: Vec<u8>,
    start: usize,
    state: ImportWriterState,
}

impl ImportWriter {
    // Consider maximum chunk size to be u64::MAX.
    // In HEX, that's 8 bytes -> 16 digits.
    // We also reserve two additional bytes for CRLF.
    const CHUNK_SIZE_RESERVED: usize = 18;
    const END_PACKET: &[u8; 5] = b"0\r\n\r\n";

    /// Ends the data import.
    /// *MUST* be called after no more data needs to be 
    /// passed to this writer.
    pub async fn finish(mut self) -> IoResult<()> {
        self.write_all(Self::END_PACKET).await
    }

    fn new(socket: ExaSocket, buffer_size: usize) -> Self {
        let buffer_size = Self::CHUNK_SIZE_RESERVED + buffer_size + 2;
        let buf = Vec::with_capacity(buffer_size);

        let mut this = Self {
            socket,
            buf,
            start: 0,
            state: ImportWriterState::Buffer,
        };

        this.empty_size();
        this
    }

    /// Writes an empty size at the beginning of the buffer and CRLF at the end.
    fn empty_size(&mut self) {
        let chunk_size = &mut self.buf[..Self::CHUNK_SIZE_RESERVED];
        chunk_size.copy_from_slice(&[0; Self::CHUNK_SIZE_RESERVED]);

        self.buf[Self::CHUNK_SIZE_RESERVED - 2] = b'\r';
        self.buf[Self::CHUNK_SIZE_RESERVED - 1] = b'\n';

        let cap = self.buf.capacity();
        self.buf[cap - 2] = b'\r';
        self.buf[cap - 1] = b'\n';
    }

    /// Flushes the data in the buffer to the underlying writer.
    /// Since we buffer chuncks, the chunk size and CRLF are added
    /// at the start and end, respectively.
    fn flush_buf(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        let len = self.buf.len();

        // If we haven't written anything yet,
        // set the size at the beginning of the buffer.
        //
        // We also need to figure out where the chunk size HEX bytes start,
        // as they could be prefixed with '0's.
        //
        // This must be done exactly once for every chunk.
        //
        // We can rely on the start == 0 since this is only set
        // after a chunk starts getting written to the underlying writer
        // and only gets reset after the entire buffer write is over.
        if self.start == 0 {
            self.empty_size();
            let mut start = 0;
            let bytes = len.to_le_bytes();

            // We iterate in REVERSE so the last non-zero byte
            // we encounter becomes the start of the chunk size.
            let iter = self.buf[..Self::CHUNK_SIZE_RESERVED - 2]
                .iter_mut()
                .enumerate()
                .rev();

            for (idx, b) in iter {
                // Byte of chunk size
                let c = bytes[idx / 2];

                // Compute HEX byte value
                let val = if idx % 2 == 1 { c % 16 } else { c / 16 };
                *b = val;

                // If it's non-zero, set this as start
                if val != 0 {
                    start = idx;
                }
            }

            // Set the start of the chunk size
            // to the last recorded non-zery byte position.
            self.start = start;
        }

        let mut this = self.project();

        // While we haven't written the whole buffer
        while *this.start < len {
            let res = ready!(this
                .socket
                .as_mut()
                .poll_write(cx, &this.buf[*this.start..]));

            match res {
                Ok(0) => {
                    let msg = "failed to write the buffered data";
                    let err = IoError::new(IoErrorKind::WriteZero, msg);
                    return Poll::Ready(Err(err));
                }
                Ok(n) => *this.start += n,
                Err(e) => return Poll::Ready(Err(e)),
            }
        }

        if *this.start > 0 {
            this.buf.drain(..*this.start);
        }

        *this.start = 0;
        *this.state = ImportWriterState::Buffer;

        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for ImportWriter {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<IoResult<usize>> {
        loop {
            match &self.state {
                ImportWriterState::Buffer => {
                    // We keep extra capacity for the chunk terminator
                    let buf_free = self.buf.capacity() - 2;
                    let max_write = cmp::min(self.buf.len() + buf.len(), buf_free);

                    // There's still space in buffer
                    if max_write > 0 {
                        return Poll::Ready(Write::write(&mut self.buf, &buf[..max_write]));
                    }

                    // Buffer is full.
                    self.state = ImportWriterState::Send;
                }
                ImportWriterState::Send => ready!(self.as_mut().flush_buf(cx))?,
            };
        }
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<IoResult<()>> {
        ready!(self.as_mut().flush_buf(cx))?;
        self.project().socket.poll_flush(cx)
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<IoResult<()>> {
        self.project().socket.poll_close(cx)
    }
}

#[derive(Debug, Copy, Clone)]
enum ImportWriterState {
    Buffer,
    Send,
}

#[pin_project]
#[derive(Debug)]
pub struct ExaImport {
    #[pin]
    socket: ExaSocket,
    compression: bool,
}
