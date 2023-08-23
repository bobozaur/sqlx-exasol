use futures_io::AsyncWrite;
use futures_util::{io::BufReader, AsyncWriteExt};
use pin_project::pin_project;

use std::{
    io::{Error as IoError, ErrorKind as IoErrorKind, Result as IoResult},
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
    buf_start: Option<usize>,
    state: WriterState,
}

impl ImportWriter {
    // Consider maximum chunk size to be u64::MAX.
    // In HEX, that's 8 bytes -> 16 digits.
    // We also reserve two additional bytes for CRLF.
    const CHUNK_SIZE_RESERVED: usize = 18;
    const END_PACKET: &[u8; 5] = b"0\r\n\r\n";

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
            state: WriterState::SkipHeaders([0; 4]),
        }
    }

    /// Writes the final empty chunk to the chunked transfer stream.
    pub async fn finish(&mut self) -> IoResult<()> {
        self.flush().await?;
        self.socket.write_all(Self::END_PACKET).await
    }

    /// Flushes the data in the buffer to the underlying writer.
    fn poll_flush_buffer(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        // Do not write an empty chunk as that would terminate the stream.
        if self.is_empty() {
            return Poll::Ready(Ok(()));
        }

        let mut this = self.project();

        if let Some(start) = this.buf_start {
            while *start < this.buf.len() {
                tracing::info!("{:?}", &this.buf[*start..]);
                let res = ready!(this.socket.as_mut().poll_write(cx, &this.buf[*start..]));

                match res {
                    Ok(0) => return write_zero_error(),
                    Ok(n) => *start += n,
                    Err(e) => return Poll::Ready(Err(e)),
                }
            }

            this.buf.truncate(Self::CHUNK_SIZE_RESERVED);
            *this.buf_start = None;
        }

        Poll::Ready(Ok(()))
    }

    /// Patches the buffer to arrange the chunk size at the start
    /// and append CR LF at the end.
    fn patch_buffer(&mut self) {
        let chunk_size = format!("{:X}\r\n", self.buffer_len()).into_bytes();
        let offset = Self::CHUNK_SIZE_RESERVED - chunk_size.len();

        self.buf[offset..Self::CHUNK_SIZE_RESERVED].clone_from_slice(&chunk_size);
        self.buf.extend_from_slice(b"\r\n");
        self.buf_start = Some(offset);
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
            let mut this = self.as_mut().project();
            match this.state {
                WriterState::SkipHeaders(buf) => {
                    let byte = ready!(poll_read_byte(this.socket.as_mut(), cx))?;

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
                WriterState::WriteResponse(offset) => {
                    // Check if response headers were written entirely
                    if *offset >= SUCCESS_HEADERS.len() {
                        *this.state = WriterState::BufferData;
                    } else {
                        let buf = &SUCCESS_HEADERS[*offset..];
                        let num_bytes = ready!(this.socket.poll_write(cx, buf))?;
                        *offset += num_bytes;
                    }
                }
                WriterState::BufferData => {
                    tracing::info!("buffering data");
                    // We keep extra capacity for the chunk terminator
                    let cap = this.buf.capacity() - 2;

                    // There's still space in buffer
                    if cap > this.buf.len() {
                        let res = Pin::new(this.buf).poll_write(cx, buf);
                        tracing::info!("buf write res: {res:?}");
                        return res;
                    }

                    tracing::info!("data buffered!");

                    // Buffer is full, patch and send it.
                    *this.state = WriterState::PatchBuffer;
                }
                WriterState::PatchBuffer => {
                    tracing::info!("patching buffer");
                    self.patch_buffer();
                    self.state = WriterState::Send;
                }

                WriterState::Send => {
                    ready!(self.as_mut().poll_flush_buffer(cx))?;
                    tracing::info!("buffer sent");
                    self.state = WriterState::BufferData;
                }
            };
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        ready!(self.as_mut().poll_flush_buffer(cx))?;
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
    BufferData,
    PatchBuffer,
    Send,
}

fn write_zero_error() -> Poll<IoResult<()>> {
    let err = IoError::new(IoErrorKind::WriteZero, "failed to write the buffered data");
    Poll::Ready(Err(err))
}
