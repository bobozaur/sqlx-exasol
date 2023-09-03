use std::{
    io::Result as IoResult,
    pin::Pin,
    task::{Context, Poll},
};

#[cfg(feature = "compression")]
use async_compression::futures::write::GzipEncoder;
use futures_io::AsyncWrite;
use pin_project::pin_project;

use super::writer::ImportWriter;
use crate::connection::websocket::socket::ExaSocket;

#[pin_project(project = ExaImportWriterProj)]
#[derive(Debug)]
pub enum ExaImportWriter {
    Plain(#[pin] ImportWriter),
    #[cfg(feature = "compression")]
    Compressed(#[pin] GzipEncoder<ImportWriter>),
}

impl ExaImportWriter {
    pub fn new(socket: ExaSocket, buffer_size: usize, with_compression: bool) -> Self {
        let writer = ImportWriter::new(socket, buffer_size);

        match with_compression {
            #[cfg(feature = "compression")]
            true => Self::Compressed(GzipEncoder::new(writer)),
            _ => Self::Plain(writer),
        }
    }
}

impl AsyncWrite for ExaImportWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<IoResult<usize>> {
        match self.as_mut().project() {
            #[cfg(feature = "compression")]
            ExaImportWriterProj::Compressed(s) => s.poll_write(cx, buf),
            ExaImportWriterProj::Plain(s) => s.poll_write(cx, buf),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        match self.as_mut().project() {
            #[cfg(feature = "compression")]
            ExaImportWriterProj::Compressed(s) => s.poll_flush(cx),
            ExaImportWriterProj::Plain(s) => s.poll_flush(cx),
        }
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        match self.as_mut().project() {
            #[cfg(feature = "compression")]
            ExaImportWriterProj::Compressed(s) => s.poll_close(cx),
            ExaImportWriterProj::Plain(s) => s.poll_close(cx),
        }
    }
}