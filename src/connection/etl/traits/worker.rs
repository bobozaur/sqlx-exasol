use std::{
    io::Result as IoResult,
    pin::Pin,
    task::{ready, Context, Poll},
};

use futures_io::{AsyncBufRead, AsyncRead, AsyncWrite};
use futures_util::io::BufReader;

use crate::{connection::websocket::socket::ExaSocket, etl::error::ExaEtlError};

impl Worker for BufReader<ExaSocket> {}

pub trait Worker: AsyncBufRead + AsyncRead + AsyncWrite {
    const DOUBLE_CR_LF: &'static [u8; 4] = b"\r\n\r\n";
    const CR: u8 = b'\r';
    const LF: u8 = b'\n';

    fn poll_read_byte(self: Pin<&mut Self>, cx: &mut Context) -> Poll<IoResult<u8>> {
        let mut buffer = [0; 1];
        let n = ready!(self.poll_read(cx, &mut buffer))?;

        if n != 1 {
            Err(ExaEtlError::ByteRead)?
        } else {
            Poll::Ready(Ok(buffer[0]))
        }
    }

    /// Sends some static data, returning whether all of it was sent or not.
    fn poll_send_static(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &'static [u8],
        offset: &mut usize,
    ) -> Poll<IoResult<bool>> {
        if *offset >= buf.len() {
            return Poll::Ready(Ok(true));
        }

        let num_bytes = ready!(self.poll_write(cx, &buf[*offset..]))?;
        *offset += num_bytes;

        Poll::Ready(Ok(false))
    }

    fn poll_until_double_crlf(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [u8; 4],
    ) -> Poll<IoResult<bool>> {
        let byte = ready!(self.poll_read_byte(cx))?;

        // Shift bytes
        buf[0] = buf[1];
        buf[1] = buf[2];
        buf[2] = buf[3];
        buf[3] = byte;

        // If true, all headers have been read
        match buf == Self::DOUBLE_CR_LF {
            true => Poll::Ready(Ok(true)),
            false => Poll::Ready(Ok(false)),
        }
    }

    fn poll_read_cr(self: Pin<&mut Self>, cx: &mut Context) -> Poll<IoResult<()>> {
        let byte = ready!(self.poll_read_byte(cx))?;

        if byte != Self::CR {
            Err(ExaEtlError::InvalidByte(Self::CR, byte))?
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_read_lf(self: Pin<&mut Self>, cx: &mut Context) -> Poll<IoResult<()>> {
        let byte = ready!(self.poll_read_byte(cx))?;

        if byte != Self::LF {
            Err(ExaEtlError::InvalidByte(Self::LF, byte))?
        } else {
            Poll::Ready(Ok(()))
        }
    }
}
