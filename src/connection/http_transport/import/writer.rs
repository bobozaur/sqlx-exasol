use arrayvec::ArrayString;
use futures_io::AsyncWrite;
use futures_util::io::BufReader;
use pin_project::pin_project;

use std::{
    fmt::Write,
    io::{Error as IoError, ErrorKind as IoErrorKind, Result as IoResult},
    pin::Pin,
    task::{ready, Context, Poll},
};

use crate::connection::{
    http_transport::{poll_ignore_headers, poll_send_static},
    websocket::socket::ExaSocket,
};

#[pin_project]
#[derive(Debug)]
pub struct ImportWriter {
    #[pin]
    socket: BufReader<ExaSocket>,
    buf: Vec<u8>,
    buf_start: Option<usize>,
    buf_patched: bool,
    state: WriterState,
}

impl ImportWriter {
    // Consider maximum chunk size to be u64::MAX.
    // In HEX, that's 8 bytes -> 16 digits.
    // We also reserve two additional bytes for CRLF.
    const CHUNK_SIZE_RESERVED: usize = 18;
    const EMPTY_CHUNK: &[u8; 5] = b"0\r\n\r\n";

    /// HTTP Response for the IMPORT request Exasol sends.
    const RESPONSE: &[u8; 66] = b"HTTP/1.1 200 OK\r\n\
                                  Connection: close\r\n\
                                  Transfer-Encoding: chunked\r\n\
                                  \r\n";

    pub fn new(socket: ExaSocket, buffer_size: usize) -> Self {
        let buffer_size = Self::CHUNK_SIZE_RESERVED + buffer_size + 2;
        let mut buf = Vec::with_capacity(buffer_size);

        // Set the size bytes to the inital, empty chunk state.
        buf.extend_from_slice(&[0; Self::CHUNK_SIZE_RESERVED]);
        buf[Self::CHUNK_SIZE_RESERVED - 2] = b'\r';
        buf[Self::CHUNK_SIZE_RESERVED - 1] = b'\n';

        Self {
            socket: BufReader::new(socket),
            buf,
            buf_start: None,
            buf_patched: false,
            state: WriterState::SkipHeaders([0; 4]),
        }
    }

    /// Flushes the data in the buffer to the underlying writer.
    fn poll_flush_buffer(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        // Do not write an empty chunk as that would terminate the stream.
        if self.is_empty() {
            return Poll::Ready(Ok(()));
        }

        // Must only happen once per chunk.
        if !self.buf_patched {
            self.as_mut().patch_buffer();
        }

        let mut this = self.project();

        if let Some(start) = this.buf_start {
            while *start < this.buf.len() {
                let res = ready!(this.socket.as_mut().poll_write(cx, &this.buf[*start..]));

                match res {
                    Ok(0) => return write_zero_error(),
                    Ok(n) => *start += n,
                    Err(e) => return Poll::Ready(Err(e)),
                }
            }

            this.buf.truncate(Self::CHUNK_SIZE_RESERVED);
            *this.buf_start = None;
            *this.buf_patched = false;
        }

        Poll::Ready(Ok(()))
    }

    /// Patches the buffer to arrange the chunk size at the start
    /// and append CR LF at the end.
    fn patch_buffer(mut self: Pin<&mut Self>) {
        let mut chunk_size_str = ArrayString::<{ Self::CHUNK_SIZE_RESERVED }>::new_const();
        write!(&mut chunk_size_str, "{:X}\r\n", self.buffer_len())
            .expect("buffer shouldn't be bigger than u64::MAX");

        let chunk_size = chunk_size_str.as_bytes();
        let offset = Self::CHUNK_SIZE_RESERVED - chunk_size.len();

        self.buf[offset..Self::CHUNK_SIZE_RESERVED].clone_from_slice(chunk_size);
        self.buf.extend_from_slice(b"\r\n");
        self.buf_start = Some(offset);
        self.buf_patched = true;
    }

    fn buffer_len(&self) -> usize {
        self.buf.len() - Self::CHUNK_SIZE_RESERVED
    }

    fn is_empty(&self) -> bool {
        self.buffer_len() == 0
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
                    let done = ready!(poll_ignore_headers(this.socket, cx, buf))?;

                    // If true, all headers have been read
                    if done {
                        *this.state = WriterState::WriteResponse(0);
                    }
                }
                WriterState::WriteResponse(offset) => {
                    let done = ready!(poll_send_static(this.socket, cx, Self::RESPONSE, offset))?;

                    if done {
                        *this.state = WriterState::BufferData;
                    }
                }
                WriterState::BufferData => {
                    // We keep extra capacity for the chunk terminator
                    let buf_free = this.buf.capacity() - this.buf.len() - 2;

                    // There's still space in buffer
                    if buf_free > 0 {
                        let max_write = buf_free.min(buf.len());
                        return Pin::new(this.buf).poll_write(cx, &buf[..max_write]);
                    }

                    // Buffer is full, send it.
                    *this.state = WriterState::Send;
                }

                WriterState::Send => {
                    ready!(self.as_mut().poll_flush_buffer(cx))?;
                    self.state = WriterState::BufferData;
                }

                WriterState::End(offset) => {
                    let done =
                        ready!(poll_send_static(this.socket, cx, Self::EMPTY_CHUNK, offset))?;

                    if done {
                        return Poll::Ready(Ok(0));
                    }
                }
            };
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        ready!(self.as_mut().poll_flush_buffer(cx))?;
        self.project().socket.poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        ready!(self.as_mut().poll_flush(cx))?;

        // We write an empty buffer to artifically trigger the WriterState::End flow.
        self.state = WriterState::End(0);
        ready!(self.as_mut().poll_write(cx, &[]))?;

        self.project().socket.poll_close(cx)
    }
}

#[derive(Debug, Copy, Clone)]
enum WriterState {
    SkipHeaders([u8; 4]),
    WriteResponse(usize),
    BufferData,
    Send,
    End(usize),
}

fn write_zero_error() -> Poll<IoResult<()>> {
    let err = IoError::new(IoErrorKind::WriteZero, "failed to write the buffered data");
    Poll::Ready(Err(err))
}
