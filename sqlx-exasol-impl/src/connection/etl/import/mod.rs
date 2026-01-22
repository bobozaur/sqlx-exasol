mod compression;
mod options;
mod service;
mod writer;

use std::{
    fmt::Debug,
    io,
    pin::Pin,
    task::{Context, Poll},
};

use compression::MaybeCompressedWriter;
use futures_channel::mpsc::{Receiver, Sender};
use futures_io::AsyncWrite;
pub use options::ImportBuilder;
use sqlx_core::bytes::BytesMut;

use crate::etl::{import::service::ImportService, job::OneShotServer};

type ImportDataSender = Sender<BytesMut>;
type ImportDataReceiver = Receiver<BytesMut>;
type ImportChannelSender = flume::Sender<ImportDataSender>;
type ImportPartsReceiver = flume::Receiver<(ImportDataSender, OneShotServer<ImportService>)>;

/// An ETL IMPORT worker.
///
/// The type implements [`AsyncWrite`] and is [`Send`] and [`Sync`] so it can be freely used for any
/// data pipeline.
///
/// The only caveat is that you *MUST* call [`futures_util::AsyncWriteExt::close`] on each worker to
/// finalize the import. A worker that is dropped without closing will cause the HTTP connection to
/// be unexpectedly dropped and the `IMPORT` query will return an error.
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
/// use sqlx_exasol::{error::*, etl::*, *};
///
/// # async {
/// #
/// let pool = ExaPool::connect(&env::var("DATABASE_URL").unwrap()).await?;
/// let mut con = pool.acquire().await?;
/// let mut tx = con.begin().await?;
///
/// let (query_fut, writers) = ImportBuilder::new("SOME_TABLE", None)
///     .build(&mut *tx)
///     .await?;
///
/// // concurrently use the writers and await the query future
///
/// tx.commit().await?;
/// #
/// # let res: Result<(), BoxDynError> = Ok(());
/// # res
/// # };
/// ```
///
/// # IMPORTANT
///
/// During `IMPORT` jobs Exasol will make a series of HTTP requests to the driver, one for each
/// worker. An `IMPORT` query is considered successful by Exasol if at least one HTTP response from
/// the driver contains data. This means at least one [`crate::connection::etl::ExaImport`] worker
/// (regardless which) must write some data before being closed.
#[derive(Debug)]
pub struct ExaImport(MaybeCompressedWriter);

impl ExaImport {
    fn new(rx: ImportPartsReceiver, buffer_size: usize, with_compression: bool) -> Self {
        Self(MaybeCompressedWriter::new(
            rx,
            buffer_size,
            with_compression,
        ))
    }
}

impl AsyncWrite for ExaImport {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write_vectored(cx, bufs)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_close(cx)
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
