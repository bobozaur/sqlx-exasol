mod options;
mod writer;

use std::{
    io::Result as IoResult,
    pin::Pin,
    task::{ready, Context, Poll},
};

#[cfg(feature = "compression")]
use async_compression::futures::write::GzipEncoder;
use futures_core::future::BoxFuture;
use futures_io::AsyncWrite;
use futures_util::FutureExt;
use pin_project::pin_project;

use writer::ImportWriter;

pub use options::{ImportBuilder, Trim};

use crate::connection::websocket::socket::ExaSocket;

#[pin_project(project = ExaImportProj)]
pub enum ExaImport {
    Setup(#[pin] BoxFuture<'static, IoResult<ExaSocket>>, usize, bool),
    Plain(#[pin] ImportWriter),
    #[cfg(feature = "compression")]
    Compressed(#[pin] GzipEncoder<ImportWriter>),
}

impl ExaImport {
    fn make_writer(socket: ExaSocket, buffer_size: usize, with_compression: bool) -> Self {
        let writer = ImportWriter::new(socket, buffer_size);

        match with_compression {
            #[cfg(feature = "compression")]
            true => Self::Compressed(GzipEncoder::new(writer)),
            _ => Self::Plain(writer),
        }
    }
}

impl AsyncWrite for ExaImport {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<IoResult<usize>> {
        loop {
            let (socket, buffer_size, with_compression) = match self.as_mut().project() {
                #[cfg(feature = "compression")]
                ExaImportProj::Compressed(s) => return s.poll_write(cx, buf),
                ExaImportProj::Plain(s) => return s.poll_write(cx, buf),
                ExaImportProj::Setup(mut f, s, c) => (ready!(f.poll_unpin(cx))?, *s, *c),
            };

            self.set(Self::make_writer(socket, buffer_size, with_compression))
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        loop {
            let (socket, buffer_size, with_compression) = match self.as_mut().project() {
                #[cfg(feature = "compression")]
                ExaImportProj::Compressed(s) => return s.poll_flush(cx),
                ExaImportProj::Plain(s) => return s.poll_flush(cx),
                ExaImportProj::Setup(mut f, s, c) => (ready!(f.poll_unpin(cx))?, *s, *c),
            };

            self.set(Self::make_writer(socket, buffer_size, with_compression))
        }
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        loop {
            let (socket, buffer_size, with_compression) = match self.as_mut().project() {
                #[cfg(feature = "compression")]
                ExaImportProj::Compressed(s) => return s.poll_close(cx),
                ExaImportProj::Plain(s) => return s.poll_close(cx),
                ExaImportProj::Setup(mut f, s, c) => (ready!(f.poll_unpin(cx))?, *s, *c),
            };

            self.set(Self::make_writer(socket, buffer_size, with_compression))
        }
    }
}
