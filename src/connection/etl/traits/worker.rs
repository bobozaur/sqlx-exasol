use std::{
    io::Result as IoResult,
    pin::Pin,
    task::{ready, Context, Poll},
};

use futures_io::{AsyncBufRead, AsyncRead, AsyncWrite};
use futures_util::io::BufReader;

use crate::{connection::websocket::socket::ExaSocket, etl::error::ExaEtlError};

impl EtlWorker for BufReader<ExaSocket> {}

/// Trait implemented for ETL IO workers, providing common methods
/// useful for both IMPORT and EXPORT operations.
pub trait EtlWorker: AsyncBufRead + AsyncRead + AsyncWrite {
    const DOUBLE_CR_LF: &'static [u8; 4] = b"\r\n\r\n";
    const CR: u8 = b'\r';
    const LF: u8 = b'\n';

    fn poll_read_byte(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<u8>> {
        let mut buffer = [0; 1];

        loop {
            let n = ready!(self.as_mut().poll_read(cx, &mut buffer))?;

            if n == 1 {
                return Poll::Ready(Ok(buffer[0]));
            }
        }
    }

    /// Sends some static data, returning whether all of it was sent or not.
    fn poll_send_static(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &'static [u8],
        offset: &mut usize,
    ) -> Poll<IoResult<()>> {
        loop {
            if *offset >= buf.len() {
                return Poll::Ready(Ok(()));
            }

            let num_bytes = ready!(self.as_mut().poll_write(cx, &buf[*offset..]))?;
            *offset += num_bytes;
        }
    }

    fn poll_until_double_crlf(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8; 4],
    ) -> Poll<IoResult<()>> {
        loop {
            let byte = ready!(self.as_mut().poll_read_byte(cx))?;

            // Shift bytes
            buf[0] = buf[1];
            buf[1] = buf[2];
            buf[2] = buf[3];
            buf[3] = byte;

            // If true, all headers have been read
            if buf == Self::DOUBLE_CR_LF {
                return Poll::Ready(Ok(()));
            }
        }
    }

    fn poll_read_cr(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        let byte = ready!(self.poll_read_byte(cx))?;

        if byte == Self::CR {
            Poll::Ready(Ok(()))
        } else {
            Err(ExaEtlError::InvalidByte(Self::CR, byte))?
        }
    }

    fn poll_read_lf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        let byte = ready!(self.poll_read_byte(cx))?;

        if byte == Self::LF {
            Poll::Ready(Ok(()))
        } else {
            Err(ExaEtlError::InvalidByte(Self::LF, byte))?
        }
    }
}
