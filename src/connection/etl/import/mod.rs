mod compression;
mod options;
mod writer;

use std::{
    fmt::Debug,
    io::Result as IoResult,
    pin::Pin,
    task::{ready, Context, Poll},
};

use compression::ExaImportWriter;
use futures_io::AsyncWrite;
use futures_util::FutureExt;
pub use options::{ImportBuilder, Trim};
use pin_project::pin_project;

use super::SocketFuture;

#[allow(clippy::large_enum_variant)]
#[pin_project(project = ExaImportProj)]
pub enum ExaImport {
    Setup(#[pin] SocketFuture, usize, bool),
    Writing(#[pin] ExaImportWriter),
}

impl Debug for ExaImport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Setup(..) => f.debug_tuple("Setup").finish(),
            Self::Writing(arg0) => f.debug_tuple("Writing").field(arg0).finish(),
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
                ExaImportProj::Writing(s) => return s.poll_write(cx, buf),
                ExaImportProj::Setup(mut f, s, c) => (ready!(f.poll_unpin(cx))?, *s, *c),
            };

            let writer = ExaImportWriter::new(socket, buffer_size, with_compression);
            self.set(Self::Writing(writer))
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        loop {
            let (socket, buffer_size, with_compression) = match self.as_mut().project() {
                ExaImportProj::Writing(s) => return s.poll_flush(cx),
                ExaImportProj::Setup(mut f, s, c) => (ready!(f.poll_unpin(cx))?, *s, *c),
            };

            let writer = ExaImportWriter::new(socket, buffer_size, with_compression);
            self.set(Self::Writing(writer))
        }
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        loop {
            let (socket, buffer_size, with_compression) = match self.as_mut().project() {
                ExaImportProj::Writing(s) => return s.poll_close(cx),
                ExaImportProj::Setup(mut f, s, c) => (ready!(f.poll_unpin(cx))?, *s, *c),
            };

            let writer = ExaImportWriter::new(socket, buffer_size, with_compression);
            self.set(Self::Writing(writer))
        }
    }
}
