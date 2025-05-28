use std::{
    fmt::Debug,
    io::Result as IoResult,
    pin::Pin,
    task::{Context, Poll},
};

#[cfg(feature = "compression")]
use async_compression::futures::write::GzipEncoder;
use futures_io::AsyncWrite;

use super::writer::ExaWriter;
use crate::connection::websocket::socket::ExaSocket;

/// Wrapper enum that handles the compression support for the [`ExaWriter`].
#[derive(Debug)]
pub enum ExaImportWriter {
    Plain(ExaWriter),
    #[cfg(feature = "compression")]
    Compressed(GzipEncoder<ExaWriter>),
}

impl ExaImportWriter {
    pub fn new(socket: ExaSocket, buffer_size: usize, with_compression: bool) -> Self {
        let writer = ExaWriter::new(socket, buffer_size);

        match with_compression {
            #[cfg(feature = "compression")]
            true => Self::Compressed(GzipEncoder::new(writer)),
            _ => Self::Plain(writer),
        }
    }
}

impl AsyncWrite for ExaImportWriter {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<IoResult<usize>> {
        match self.get_mut() {
            #[cfg(feature = "compression")]
            Self::Compressed(s) => Pin::new(s).poll_write(cx, buf),
            Self::Plain(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        match self.get_mut() {
            #[cfg(feature = "compression")]
            Self::Compressed(s) => Pin::new(s).poll_flush(cx),
            Self::Plain(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        match self.get_mut() {
            #[cfg(feature = "compression")]
            Self::Compressed(s) => Pin::new(s).poll_close(cx),
            Self::Plain(s) => Pin::new(s).poll_close(cx),
        }
    }
}
