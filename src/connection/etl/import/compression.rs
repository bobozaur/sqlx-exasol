use std::{
    fmt::Debug,
    io::Result as IoResult,
    pin::Pin,
    task::{Context, Poll},
};

#[cfg(feature = "compression")]
use async_compression::futures::write::GzipEncoder;
use futures_io::AsyncWrite;
use pin_project::pin_project;

use crate::connection::websocket::socket::ExaSocket;

use super::writer::ExaWriter;

/// Wrapper enum that handles the compression support for the [`ExaWriter`].
#[pin_project(project = ExaExaWriterProj)]
#[derive(Debug)]
pub enum ExaImportWriter {
    Plain(#[pin] ExaWriter),
    #[cfg(feature = "compression")]
    Compressed(#[pin] GzipEncoder<ExaWriter>),
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
        match self.project() {
            #[cfg(feature = "compression")]
            ExaExaWriterProj::Compressed(s) => s.poll_write(cx, buf),
            ExaExaWriterProj::Plain(s) => s.poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        match self.project() {
            #[cfg(feature = "compression")]
            ExaExaWriterProj::Compressed(s) => s.poll_flush(cx),
            ExaExaWriterProj::Plain(s) => s.poll_flush(cx),
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        match self.project() {
            #[cfg(feature = "compression")]
            ExaExaWriterProj::Compressed(s) => s.poll_close(cx),
            ExaExaWriterProj::Plain(s) => s.poll_close(cx),
        }
    }
}
