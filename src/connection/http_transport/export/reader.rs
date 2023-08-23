use std::{
    fmt::Debug,
    io::{Error as IoError, ErrorKind as IoErrorKind, Result as IoResult},
    pin::Pin,
    task::{ready, Context, Poll},
};

use crate::connection::{
    http_transport::{poll_read_byte, poll_send_static, poll_until_double_crlf},
    websocket::socket::ExaSocket,
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
    ended: bool,
}

impl ExportReader {
    /// Packet that tells Exasol the transport was successfull.
    const RESPONSE: &[u8; 38] = b"HTTP/1.1 200 OK\r\n\
                                  Connection: close\r\n\
                                  \r\n";

    pub fn new(socket: ExaSocket) -> Self {
        Self {
            socket: BufReader::new(socket),
            state: ReaderState::SkipRequest([0; 4]),
            chunk_size: 0,
            ended: false,
        }
    }

    fn poll_read_cr(
        socket: Pin<&mut BufReader<ExaSocket>>,
        cx: &mut Context,
    ) -> Poll<IoResult<()>> {
        let byte = ready!(poll_read_byte(socket, cx))?;

        if byte != b'\r' {
            invalid_size_byte(byte)
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_read_lf(
        socket: Pin<&mut BufReader<ExaSocket>>,
        cx: &mut Context,
    ) -> Poll<IoResult<()>> {
        let byte = ready!(poll_read_byte(socket, cx))?;

        if byte != b'\n' {
            invalid_size_byte(byte)
        } else {
            Poll::Ready(Ok(()))
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
                    let done = ready!(poll_until_double_crlf(this.socket, cx, buf))?;

                    if done {
                        *this.state = ReaderState::ReadSize;
                    }
                }

                ReaderState::ReadSize => {
                    let byte = ready!(poll_read_byte(this.socket, cx))?;

                    let digit = match byte {
                        b'0'..=b'9' => byte - b'0',
                        b'a'..=b'f' => 10 + byte - b'a',
                        b'A'..=b'F' => 10 + byte - b'A',
                        b'\r' => {
                            *this.state = ReaderState::ExpectSizeLF;
                            continue;
                        }
                        _ => {
                            return invalid_size_byte(byte);
                        }
                    };

                    *this.chunk_size = this
                        .chunk_size
                        .checked_mul(16)
                        .ok_or_else(overflow)?
                        .checked_add(digit.into())
                        .ok_or_else(overflow)?;
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
                    ready!(Self::poll_read_lf(this.socket, cx))?;
                    // If even after encountering CR the chunk size is still 0,
                    // then we reached the end of the stream.
                    if *this.chunk_size == 0 {
                        *this.ended = true;
                    }

                    *this.state = ReaderState::ReadData;
                }

                ReaderState::ExpectDataCR => {
                    ready!(Self::poll_read_cr(this.socket, cx))?;
                    *this.state = ReaderState::ExpectDataLF;
                }

                ReaderState::ExpectDataLF => {
                    ready!(Self::poll_read_lf(this.socket, cx))?;
                    if *this.ended {
                        *this.state = ReaderState::WriteResponse(0);
                    } else {
                        *this.state = ReaderState::ReadSize;
                    }
                }

                ReaderState::WriteResponse(offset) => {
                    // EOF is reached after writing the HTTP response.
                    let socket = this.socket.as_mut();
                    let done = ready!(poll_send_static(socket, cx, Self::RESPONSE, offset))?;

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
    ExpectDataCR,
    ExpectDataLF,
    ExpectSizeLF,
    WriteResponse(usize),
}

fn invalid_size_byte<T>(byte: u8) -> Poll<IoResult<T>> {
    let msg = format!("expected HEX or CR byte, found {byte}");
    Poll::Ready(Err(IoError::new(IoErrorKind::InvalidData, msg)))
}

fn overflow() -> IoError {
    IoError::new(IoErrorKind::InvalidData, "chunk size overflowed 64 bits")
}
