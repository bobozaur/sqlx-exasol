mod compression;
mod options;
mod trim;
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
pub use options::ImportBuilder;
use pin_project::pin_project;
pub use trim::Trim;

use super::SocketFuture;

/// An ETL IMPORT worker.
///
/// The type implements [`AsyncWrite`] and is [`Send`] and [`Sync`] so it can be (almost) freely
/// used for any data pipeline.
///
/// The only caveat is that you *MUST* call [`futures_util::AsyncWriteExt::close`] on each worker to
/// finalize the import. Otherwise, Exasol keeps on expecting data.
///
/// # IMPORTANT
///
/// In multi-node environments closing a writer without writing any data to it can
/// cause issues - Exasol does not immediately start reading data from all workers but rather seems
/// to start them in somewhat of an "on-demand" fashion.
///
/// If you close a worker before Exasol tries to use it a connection error will be returned by the
/// ETL driving future.
///
/// It's best not to create excess writers that you don't plan on using to avoid such issues.
#[allow(clippy::large_enum_variant)]
#[pin_project(project = ExaImportProj)]
pub enum ExaImport {
    Setup(SocketFuture, usize, bool),
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
                ExaImportProj::Setup(f, s, c) => (ready!(f.poll_unpin(cx))?, *s, *c),
            };

            let writer = ExaImportWriter::new(socket, buffer_size, with_compression);
            self.set(Self::Writing(writer));
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        loop {
            let (socket, buffer_size, with_compression) = match self.as_mut().project() {
                ExaImportProj::Writing(s) => return s.poll_flush(cx),
                ExaImportProj::Setup(f, s, c) => (ready!(f.poll_unpin(cx))?, *s, *c),
            };

            let writer = ExaImportWriter::new(socket, buffer_size, with_compression);
            self.set(Self::Writing(writer));
        }
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        loop {
            let (socket, buffer_size, with_compression) = match self.as_mut().project() {
                ExaImportProj::Writing(s) => return s.poll_close(cx),
                ExaImportProj::Setup(f, s, c) => (ready!(f.poll_unpin(cx))?, *s, *c),
            };

            let writer = ExaImportWriter::new(socket, buffer_size, with_compression);
            self.set(Self::Writing(writer));
        }
    }
}
