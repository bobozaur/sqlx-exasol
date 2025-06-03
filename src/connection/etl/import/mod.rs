#![expect(deprecated, reason = "will hide enum under a public type")]

mod compression;
mod options;
mod writer;

use std::{
    fmt::Debug,
    io,
    pin::Pin,
    task::{ready, Context, Poll},
};

use compression::ExaImportWriter;
use futures_core::future::BoxFuture;
use futures_io::AsyncWrite;
use futures_util::FutureExt;
pub use options::ImportBuilder;

use crate::connection::websocket::socket::ExaSocket;

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
/// `IMPORT` jobs are not atomic by themselves. If an error occurs during the data ingestion, some
/// of the data might be already sent and written in the database. However, since `IMPORT` is
/// fundamentally just a query, it *can* be transactional. Therefore, beginning a transaction and
/// passing that to the [`ImportBuilder::build`] method will result in the import job needing to be
/// explicitly committed:
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
/// From what I could gather from the logs, providing no data (although the request is responded to
/// gracefully) makes Exasol retry the connection. With these workers being implemented as one-shot
/// HTTP servers, there's nothing to connect to anymore. Even if it were, the connection would just
/// be re-attempted over and over since we'd still be sending no data.
///
/// Since not using one or more import workers seems to be treated as an error on Exasol's side,
/// it's best not to create excess writers that you don't plan on using to avoid such issues.
///
/// See <https://github.com/exasol/websocket-api/issues/33> for more details.
#[allow(clippy::large_enum_variant)]
pub enum ExaImport {
    /// Setup state of the worker. This typically means waiting on the TLS handshake.
    ///
    /// This approach is needed because Exasol will issue connections sequentially and thus perform
    /// TLS handshakes the same way.
    ///
    /// Therefore we accommodate the worker state until the query gets executed and data gets sent
    /// through the workers, which happens within consumer code.
    #[deprecated = "will be made private"]
    Setup(BoxFuture<'static, io::Result<ExaSocket>>, usize, bool),
    /// The worker is fully connected and ready for I/O.
    #[deprecated = "will be made private"]
    Writing(ExaImportWriter),
}

impl Debug for ExaImport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Writing(arg0) => f.debug_tuple("Writing").field(arg0).finish(),
            Self::Setup(..) => f.debug_tuple("Setup").finish(),
        }
    }
}

impl AsyncWrite for ExaImport {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        loop {
            let (socket, buffer_size, with_compression) = match self.as_mut().get_mut() {
                Self::Writing(s) => return Pin::new(s).poll_write(cx, buf),
                Self::Setup(f, s, c) => (ready!(f.poll_unpin(cx))?, *s, *c),
            };

            let writer = ExaImportWriter::new(socket, buffer_size, with_compression);
            self.set(Self::Writing(writer));
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        loop {
            let (socket, buffer_size, with_compression) = match self.as_mut().get_mut() {
                Self::Writing(s) => return Pin::new(s).poll_flush(cx),
                Self::Setup(f, s, c) => (ready!(f.poll_unpin(cx))?, *s, *c),
            };

            let writer = ExaImportWriter::new(socket, buffer_size, with_compression);
            self.set(Self::Writing(writer));
        }
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        loop {
            let (socket, buffer_size, with_compression) = match self.as_mut().get_mut() {
                Self::Writing(s) => return Pin::new(s).poll_close(cx),
                Self::Setup(f, s, c) => (ready!(f.poll_unpin(cx))?, *s, *c),
            };

            let writer = ExaImportWriter::new(socket, buffer_size, with_compression);
            self.set(Self::Writing(writer));
        }
    }
}

/// Trim options for IMPORT.
#[derive(Debug, Clone, Copy)]
pub enum Trim {
    Left,
    Right,
    Both,
}

impl AsRef<str> for Trim {
    fn as_ref(&self) -> &str {
        match self {
            Self::Left => "LTRIM",
            Self::Right => "RTRIM",
            Self::Both => "TRIM",
        }
    }
}
