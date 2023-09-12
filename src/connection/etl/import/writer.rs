use std::{
    fmt::Write,
    io::Result as IoResult,
    pin::Pin,
    task::{ready, Context, Poll},
};

use arrayvec::ArrayString;
use futures_io::AsyncWrite;
use futures_util::io::BufReader;
use pin_project::pin_project;

use crate::{
    connection::websocket::socket::ExaSocket,
    etl::{error::ExaEtlError, traits::EtlBufReader},
};

/// Low-level async writer used to send chunked HTTP data to Exasol.
/// The writer must be closed to finalize the import, as otherwise
/// Exasol will keep expecting more data.
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
    /// We do some implicit buffering as we have to parse
    /// the incoming HTTP request and ignore the headers, etc.
    ///
    /// We do that by reading one byte at a time and keeping track
    /// of what we read to walk through states.
    ///
    /// It would be higly inefficient to read a single byte from the
    /// TCP stream every time, so we instead use a small [`futures_util::io::BufReader`].
    const IMPLICIT_BUFFER_CAP: usize = 128;
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
            socket: BufReader::with_capacity(Self::IMPLICIT_BUFFER_CAP, socket),
            buf,
            buf_start: None,
            buf_patched: false,
            state: WriterState::SkipRequest([0; 4]),
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
            self.as_mut().patch_buffer()?;
        }

        let mut this = self.project();

        if let Some(start) = this.buf_start {
            while *start < this.buf.len() {
                let res = ready!(this.socket.as_mut().poll_write(cx, &this.buf[*start..]));

                match res {
                    Ok(0) => Err(ExaEtlError::WriteZero)?,
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
    fn patch_buffer(mut self: Pin<&mut Self>) -> Result<(), ExaEtlError> {
        let mut chunk_size_str = ArrayString::<{ Self::CHUNK_SIZE_RESERVED }>::new_const();
        write!(&mut chunk_size_str, "{:X}\r\n", self.buffer_len())
            .map_err(|_| ExaEtlError::ChunkSizeOverflow)?;

        let chunk_size = chunk_size_str.as_bytes();
        let offset = Self::CHUNK_SIZE_RESERVED - chunk_size.len();

        self.buf[offset..Self::CHUNK_SIZE_RESERVED].clone_from_slice(chunk_size);
        self.buf.extend_from_slice(b"\r\n");
        self.buf_start = Some(offset);
        self.buf_patched = true;

        Ok(())
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
                WriterState::SkipRequest(buf) => {
                    ready!(this.socket.poll_until_double_crlf(cx, buf))?;
                    *this.state = WriterState::WriteResponse(0);
                }

                WriterState::WriteResponse(offset) => {
                    ready!(this.socket.poll_send_static(cx, Self::RESPONSE, offset))?;
                    *this.state = WriterState::Buffer;
                }

                WriterState::Buffer => {
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
                    self.state = WriterState::Buffer;
                }

                WriterState::EmptyChunk(offset) => {
                    ready!(this.socket.poll_send_static(cx, Self::EMPTY_CHUNK, offset))?;
                    *this.state = WriterState::Ending;
                }

                WriterState::Ending => {
                    // We flush to ensure that all data has reached Exasol
                    // before we signal that the write is over.
                    ready!(this.socket.poll_flush(cx))?;
                    *this.state = WriterState::Ended;
                }

                WriterState::Ended => {
                    return Poll::Ready(Ok(0));
                }
            };
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        ready!(self.as_mut().poll_flush_buffer(cx))?;
        self.project().socket.poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        // Ensure the state machine was driven through its states correctly.
        loop {
            match self.state {
                // There's buffered data, so we must send it.
                WriterState::Buffer if !self.is_empty() => self.state = WriterState::Send,
                // No data is buffered, so we can end the HTTP response with the empty chunk.
                WriterState::Buffer if self.is_empty() => self.state = WriterState::EmptyChunk(0),
                // Empty chunk sent and flushed.
                WriterState::Ended => break,
                // We're in some other state, so do a dummy write to drive the state machine.
                _ => ready!(self.as_mut().poll_write(cx, &[])).map(|_| ())?,
            };
        }

        // Close socket.
        self.project().socket.poll_close(cx)
    }
}

#[derive(Debug, Copy, Clone)]
enum WriterState {
    SkipRequest([u8; 4]),
    WriteResponse(usize),
    Buffer,
    Send,
    EmptyChunk(usize),
    Ending,
    Ended,
}
