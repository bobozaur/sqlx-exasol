use std::{
    fmt::Debug,
    io::{Error as IoError, ErrorKind as IoErrorKind, Result as IoResult},
    pin::Pin,
    task::{ready, Context, Poll},
};

use crate::connection::{
    http_transport::{poll_read_byte, DOUBLE_CR_LF, SUCCESS_HEADERS},
    websocket::socket::ExaSocket,
};

use futures_io::{AsyncBufRead, AsyncRead, AsyncWrite, ErrorKind};
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
    pub fn new(socket: ExaSocket) -> Self {
        Self {
            socket: BufReader::new(socket),
            state: ReaderState::SkipHeaders([0; 4]),
            chunk_size: 0,
        }
    }

    fn poll_read_byte(
        socket: Pin<&mut BufReader<ExaSocket>>,
        cx: &mut Context,
    ) -> Poll<IoResult<u8>> {
        let mut buffer = [0; 1];
        let n = ready!(socket.poll_read(cx, &mut buffer))?;

        if n != 1 {
            Poll::Ready(Err(chunk_size_error()))
        } else {
            Poll::Ready(Ok(buffer[0]))
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
            let this = self.as_mut().project();

            match this.state {
                ReaderState::SkipHeaders(buf) => {
                    let byte = ready!(poll_read_byte(this.socket, cx))?;

                    // Shift bytes
                    buf[0] = buf[1];
                    buf[1] = buf[2];
                    buf[2] = buf[3];
                    buf[3] = byte;

                    // If true, all headers have been read
                    if buf == DOUBLE_CR_LF {
                        *this.state = ReaderState::ReadSize;
                    }
                }
                ReaderState::ReadSize => {
                    let byte = ready!(Self::poll_read_byte(this.socket, cx))?;
                    let digit = match byte {
                        b'0'..=b'9' => byte - b'0',
                        b'a'..=b'f' => 10 + byte - b'a',
                        b'A'..=b'F' => 10 + byte - b'A',
                        b'\r' => {
                            *this.state = ReaderState::ExpectLF;
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

                    if *this.chunk_size == 0 {
                        *this.state = ReaderState::WriteResponse(0);
                    }
                }
                ReaderState::ReadData => {
                    if *this.chunk_size > 0 {
                        let num_bytes = ready!(this.socket.poll_read(cx, buf))?;
                        *this.chunk_size -= num_bytes;
                        return Poll::Ready(Ok(num_bytes));
                    } else {
                        *this.state = ReaderState::ReadSize;
                    }
                }
                ReaderState::ExpectLF => {
                    let byte = ready!(Self::poll_read_byte(this.socket, cx))?;

                    if byte != b'\n' {
                        return invalid_size_byte(byte);
                    }

                    *this.state = ReaderState::ReadData;
                }
                ReaderState::WriteResponse(start) => {
                    // EOF is reached after writing the HTTP response.
                    if *start >= SUCCESS_HEADERS.len() - 1 {
                        return Poll::Ready(Ok(0));
                    }

                    let buf = &SUCCESS_HEADERS[*start..];
                    let num_bytes = ready!(this.socket.poll_write(cx, buf))?;
                    *start += num_bytes;
                }
            };
        }
    }
}

impl AsyncBufRead for ExportReader {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<&[u8]>> {
        self.project().socket.poll_fill_buf(cx)
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        self.project().socket.consume(amt)
    }
}

#[derive(Copy, Clone, Debug)]
enum ReaderState {
    SkipHeaders([u8; 4]),
    ReadSize,
    ReadData,
    ExpectLF,
    WriteResponse(usize),
}

fn invalid_size_byte<T>(byte: u8) -> Poll<IoResult<T>> {
    let msg = format!("expected HEX or CR byte, found {byte}");
    Poll::Ready(Err(IoError::new(IoErrorKind::InvalidData, msg)))
}

fn overflow() -> IoError {
    IoError::new(IoErrorKind::InvalidData, "Chunk size overflowed 64 bits")
}

fn chunk_size_error() -> IoError {
    IoError::new(ErrorKind::InvalidInput, "cannot read chunk size")
}
