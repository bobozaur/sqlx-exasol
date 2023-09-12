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
/// # Atomicity
///
/// `IMPORT` jobs are not atomic by themselves. If an error occurs during the data ingestion,
/// some of the data might be already sent and written in the database. However, since
/// `IMPORT` is fundamentally just a query, it *can* be transactional. Therefore,
/// beginning a transaction and passing that to the [`ImportBuilder::build`] method will result in
/// the import job needing to be explicitly committed:
///
/// ```rust,no_run
/// use std::env;
///
/// use sqlx_exasol::{etl::*, *};
///
/// # async {
/// #
/// let pool = ExaPool::connect(&env::var("DATABASE_URL").unwrap()).await?;
/// let mut con = pool.acquire().await?;
/// let mut tx = con.begin().await?;
///
/// let (query_fut, writers) = ImportBuilder::new("SOME_TABLE").build(&mut *tx).await?;
///
/// // concurrently use the writers and await the query future
///
/// tx.commit().await?;
/// #
/// # let res: anyhow::Result<()> = Ok(());
/// # res
/// # };
/// ```
///
/// # IMPORTANT
///
/// In multi-node environments closing a writer without writing any data to it can, and most likely
/// will, cause issues; Exasol does not immediately start reading data from all workers but rather
/// seems to connect to them sequentially after each of them provides some data.
///
/// From what I could gather from the logs, providing no data (although the request is
/// responded to gracefully) makes Exasol retry the connection. With these workers being
/// implemented as one-shot HTTP servers, there's nothing to connect to anymore. Even if it
/// were, it's possible the connection would just be re-attempted over and over since we'd still
/// be sending no data (I did not test this, though).
///
/// Since not using one or more import workers seems to be treated as an error on Exasol's side,
/// it's best not to create excess writers that you don't plan on using to avoid such issues.
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
