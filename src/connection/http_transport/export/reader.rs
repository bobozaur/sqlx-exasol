use std::{
    cmp,
    fmt::Debug,
    io::{Error as IoError, ErrorKind as IoErrorKind, Result as IoResult},
    pin::Pin,
    task::{ready, Context, Poll},
};

use crate::connection::{
    http_transport::{poll_read_byte, DOUBLE_CR_LF, SUCCESS_HEADERS},
    websocket::socket::ExaSocket,
};

use futures_io::{AsyncBufRead, AsyncRead, AsyncWrite};
use futures_util::io::BufReader;
use pin_project::pin_project;
use tracing::trace;

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
    pub fn new(socket: ExaSocket) -> Self {
        Self {
            socket: BufReader::new(socket),
            state: ReaderState::SkipHeaders([0; 4]),
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
                        trace!("finished reading request headers");
                        *this.state = ReaderState::ReadSize;
                    }
                }

                ReaderState::ReadSize => {
                    let byte = ready!(poll_read_byte(this.socket, cx))?;
                    trace!("size byte: {byte}");

                    let digit = match byte {
                        b'0'..=b'9' => byte - b'0',
                        b'a'..=b'f' => 10 + byte - b'a',
                        b'A'..=b'F' => 10 + byte - b'A',
                        b'\r' => {
                            trace!("will read chunk of size: {}", this.chunk_size);
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
                        let max_read = cmp::min(buf.len(), *this.chunk_size);
                        let num_bytes = ready!(this.socket.poll_read(cx, &mut buf[..max_read]))?;
                        *this.chunk_size -= num_bytes;
                        trace!("read {num_bytes} bytes remaining; {}", this.chunk_size);
                        return Poll::Ready(Ok(num_bytes));
                    } else {
                        *this.state = ReaderState::ExpectDataCR;
                    }
                }

                ReaderState::ExpectSizeLF => {
                    trace!("reading size LF!");
                    ready!(Self::poll_read_lf(this.socket, cx))?;
                    // If even after encountering CR the chunk size is still 0,
                    // then we reached the end of the stream.
                    if *this.chunk_size == 0 {
                        trace!("reader marked as ended!");
                        *this.ended = true;
                    }

                    *this.state = ReaderState::ReadData;
                }

                ReaderState::ExpectDataCR => {
                    trace!("reading data CR!");
                    ready!(Self::poll_read_cr(this.socket, cx))?;
                    *this.state = ReaderState::ExpectDataLF;
                }

                ReaderState::ExpectDataLF => {
                    trace!("reading data LF!");
                    ready!(Self::poll_read_lf(this.socket, cx))?;
                    if *this.ended {
                        trace!("reader ended; writing response");
                        *this.state = ReaderState::WriteResponse(0);
                    } else {
                        *this.state = ReaderState::ReadSize;
                    }
                }

                ReaderState::WriteResponse(start) => {
                    // EOF is reached after writing the HTTP response.
                    // But we need to wait for the query to finish execution,
                    // thus ensuring that the server has read the response.
                    if *start >= SUCCESS_HEADERS.len() {
                        trace!("polling flag");
                        ready!(this.socket.poll_flush(cx))?;
                        return Poll::Ready(Ok(0));
                    }

                    let buf = &SUCCESS_HEADERS[*start..];
                    trace!("{buf:?}");
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
