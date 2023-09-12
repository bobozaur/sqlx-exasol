use std::{
    fmt::Debug,
    io::{Read, Result as IoResult},
    pin::Pin,
    task::{ready, Context, Poll},
};

use futures_io::{AsyncBufRead, AsyncRead, AsyncWrite, IoSliceMut};
use futures_util::io::BufReader;
use pin_project::pin_project;

use crate::{
    connection::websocket::socket::ExaSocket,
    etl::{error::ExaEtlError, traits::EtlWorker},
};

/// Low-level async reader used to read chunked HTTP data from Exasol.
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

    pub fn new(socket: ExaSocket, buffer_size: usize) -> Self {
        Self {
            socket: BufReader::with_capacity(buffer_size, socket),
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
        let mut rem = ready!(self.as_mut().poll_fill_buf(cx))?;
        let nread = rem.read(buf)?;
        self.consume(nread);

        Poll::Ready(Ok(nread))
    }

    fn poll_read_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
    ) -> Poll<IoResult<usize>> {
        let mut rem = ready!(self.as_mut().poll_fill_buf(cx))?;
        let nread = rem.read_vectored(bufs)?;
        self.consume(nread);

        Poll::Ready(Ok(nread))
    }
}

impl AsyncBufRead for ExportReader {
    fn poll_fill_buf(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<&[u8]>> {
        loop {
            let mut this = self.as_mut().project();

            match this.state {
                ReaderState::SkipRequest(buf) => {
                    ready!(this.socket.poll_until_double_crlf(cx, buf))?;
                    *this.state = ReaderState::ReadSize;
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
                        let this = self.get_mut();
                        let buf = ready!(Pin::new(&mut this.socket).poll_fill_buf(cx))?;
                        let max_read = buf.len().min(this.chunk_size);
                        return Poll::Ready(Ok(&buf[..max_read]));
                    }

                    *this.state = ReaderState::ExpectDataCR;
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
                    ready!(socket.poll_send_static(cx, Self::RESPONSE, offset))?;
                    *this.state = ReaderState::End;
                }

                ReaderState::End => {
                    // We flush to ensure that all data has reached Exasol
                    // and the reader can finish or even be dropped.
                    ready!(this.socket.poll_flush(cx))?;
                    return Poll::Ready(Ok(&[]));
                }
            };
        }
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        let this = self.project();
        this.socket.consume(amt);
        *this.chunk_size -= amt;
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
    End,
}
