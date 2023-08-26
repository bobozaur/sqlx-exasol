use std::{
    fmt::Debug,
    io::Result as IoResult,
    pin::Pin,
    task::{ready, Context, Poll},
};

use crate::{
    connection::websocket::socket::ExaSocket,
    etl::{error::ExaEtlError, traits::Worker, IMPLICIT_BUFFER_CAP},
};

use futures_io::{AsyncRead, AsyncWrite};
use futures_util::io::BufReader;
use pin_project::pin_project;

#[pin_project]
#[derive(Debug)]
pub struct ExportReader {
    #[pin]
    socket: BufReader<ExaSocket>,
    state: ReaderState,
    chunk_size: usize,
}

impl ExportReader {
    /// HTTP Response for the EXPORT request Exasol sends.
    const RESPONSE: &[u8; 38] = b"HTTP/1.1 200 OK\r\n\
                                  Connection: close\r\n\
                                  \r\n";

    pub fn new(socket: ExaSocket) -> Self {
        Self {
            socket: BufReader::with_capacity(IMPLICIT_BUFFER_CAP, socket),
            state: ReaderState::SkipRequest([0; 4]),
            chunk_size: 0,
        }
    }
}

impl AsyncRead for ExportReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<IoResult<usize>> {
        loop {
            let mut this = self.as_mut().project();

            match this.state {
                ReaderState::SkipRequest(buf) => {
                    let done = ready!(this.socket.poll_until_double_crlf(cx, buf))?;

                    if done {
                        *this.state = ReaderState::ReadSize;
                    }
                }

                ReaderState::ReadSize => {
                    let byte = ready!(this.socket.poll_read_byte(cx))?;

                    let digit = match byte {
                        b'0'..=b'9' => byte - b'0',
                        b'a'..=b'f' => 10 + byte - b'a',
                        b'A'..=b'F' => 10 + byte - b'A',
                        b'\r' => {
                            *this.state = ReaderState::ExpectSizeLF;
                            continue;
                        }
                        _ => Err(ExaEtlError::InvalidChunkSizeByte(byte))?,
                    };

                    *this.chunk_size = this
                        .chunk_size
                        .checked_mul(16)
                        .and_then(|size| size.checked_add(digit.into()))
                        .ok_or(ExaEtlError::ChunkSizeOverflow)?;
                }

                ReaderState::ReadData => {
                    if *this.chunk_size > 0 {
                        let max_read = buf.len().min(*this.chunk_size);
                        let num_bytes = ready!(this.socket.poll_read(cx, &mut buf[..max_read]))?;
                        *this.chunk_size -= num_bytes;
                        return Poll::Ready(Ok(num_bytes));
                    } else {
                        *this.state = ReaderState::ExpectDataCR;
                    }
                }

                ReaderState::ExpectSizeLF => {
                    ready!(this.socket.poll_read_lf(cx))?;
                    // We just read the chunk size separator.
                    // If the chunk size is 0 at this stage, then
                    // this is the final, empty, chunk so we'll end the reader.
                    if *this.chunk_size == 0 {
                        *this.state = ReaderState::ExpectEndCR;
                    } else {
                        *this.state = ReaderState::ReadData;
                    }
                }

                ReaderState::ExpectDataCR => {
                    ready!(this.socket.poll_read_cr(cx))?;
                    *this.state = ReaderState::ExpectDataLF;
                }

                ReaderState::ExpectDataLF => {
                    ready!(this.socket.poll_read_lf(cx))?;
                    *this.state = ReaderState::ReadSize;
                }

                ReaderState::ExpectEndCR => {
                    ready!(this.socket.poll_read_cr(cx))?;
                    *this.state = ReaderState::ExpectEndLF;
                }

                ReaderState::ExpectEndLF => {
                    ready!(this.socket.poll_read_lf(cx))?;
                    *this.state = ReaderState::WriteResponse(0);
                }

                ReaderState::WriteResponse(offset) => {
                    // EOF is reached after writing the HTTP response.
                    let socket = this.socket.as_mut();
                    let done = ready!(socket.poll_send_static(cx, Self::RESPONSE, offset))?;

                    if done {
                        // We flush to ensure that all data has reached Exasol
                        // and the reader can finish or even be dropped.
                        ready!(this.socket.poll_flush(cx))?;
                        return Poll::Ready(Ok(0));
                    }
                }
            };
        }
    }
}

#[derive(Copy, Clone, Debug)]
enum ReaderState {
    SkipRequest([u8; 4]),
    ReadSize,
    ReadData,
    ExpectSizeLF,
    ExpectDataCR,
    ExpectDataLF,
    ExpectEndCR,
    ExpectEndLF,
    WriteResponse(usize),
}
