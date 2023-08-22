use futures_io::AsyncWrite;
use futures_util::io::BufReader;
use pin_project::pin_project;

use std::{
    cmp,
    io::{Error as IoError, ErrorKind as IoErrorKind, Result as IoResult, Write},
    pin::Pin,
    task::{ready, Context, Poll},
};

use crate::connection::{
    http_transport::{poll_read_byte, DOUBLE_CR_LF, SUCCESS_HEADERS},
    websocket::socket::ExaSocket,
};

#[pin_project]
#[derive(Debug)]
pub struct ImportWriter {
    #[pin]
    socket: BufReader<ExaSocket>,
    buf: Vec<u8>,
    start: usize,
    state: WriterState,
}

impl ImportWriter {
    // Consider maximum chunk size to be u64::MAX.
    // In HEX, that's 8 bytes -> 16 digits.
    // We also reserve two additional bytes for CRLF.
    const CHUNK_SIZE_RESERVED: usize = 18;

    pub fn new(socket: ExaSocket, buffer_size: usize) -> Self {
        let buffer_size = Self::CHUNK_SIZE_RESERVED + buffer_size + 2;
        let buf = Vec::with_capacity(buffer_size);

        let mut this = Self {
            socket: BufReader::new(socket),
            buf,
            start: 0,
            state: WriterState::SkipHeaders([0; 4]),
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
                Ok(0) => return write_zero_error(),
                Ok(n) => *this.start += n,
                Err(e) => return Poll::Ready(Err(e)),
            }
        }

        if *this.start > 0 {
            this.buf.drain(..*this.start);
        }

        *this.start = 0;
        *this.state = WriterState::Buffer;

        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for ImportWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<IoResult<usize>> {
        loop {
            let this = self.as_mut().project();
            match this.state {
                WriterState::SkipHeaders(buf) => {
                    let byte = ready!(poll_read_byte(this.socket, cx))?;

                    // Shift bytes
                    buf[0] = buf[1];
                    buf[1] = buf[2];
                    buf[2] = buf[3];
                    buf[3] = byte;

                    // If true, all headers have been read
                    if buf == DOUBLE_CR_LF {
                        *this.state = WriterState::WriteResponse(0);
                    }
                }
                WriterState::WriteResponse(start) => {
                    if *start >= SUCCESS_HEADERS.len() - 1 {
                        *this.state = WriterState::Buffer;
                    } else {
                        let buf = &SUCCESS_HEADERS[*start..];
                        let num_bytes = ready!(this.socket.poll_write(cx, buf))?;
                        *start += num_bytes;
                    }
                }
                WriterState::Buffer => {
                    // We keep extra capacity for the chunk terminator
                    let buf_free = this.buf.capacity() - 2;
                    let max_write = cmp::min(this.buf.len() + buf.len(), buf_free);

                    // There's still space in buffer
                    if max_write > 0 {
                        return Poll::Ready(Write::write(this.buf, &buf[..max_write]));
                    }

                    // Buffer is full.
                    *this.state = WriterState::Send;
                }
                WriterState::Send => ready!(self.as_mut().flush_buf(cx))?,
            };
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        ready!(self.as_mut().flush_buf(cx))?;
        self.project().socket.poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        self.project().socket.poll_close(cx)
    }
}

#[derive(Debug, Copy, Clone)]
enum WriterState {
    SkipHeaders([u8; 4]),
    WriteResponse(usize),
    Buffer,
    Send,
}

fn write_zero_error() -> Poll<IoResult<()>> {
    let err = IoError::new(IoErrorKind::WriteZero, "failed to write the buffered data");
    Poll::Ready(Err(err))
}
