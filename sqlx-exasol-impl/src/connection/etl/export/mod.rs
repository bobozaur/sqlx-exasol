mod compression;
mod options;
mod reader;
mod service;

use std::{
    fmt::Debug,
    io,
    pin::Pin,
    task::{Context, Poll},
};

use compression::MaybeCompressedReader;
use futures_channel::mpsc::{Receiver, Sender};
use futures_io::AsyncRead;
use hyper::body::Bytes;
pub use options::ExportBuilder;

use crate::etl::{export::service::ExportService, job::OneShotServer};

type ExportDataSender = Sender<Bytes>;
type ExportDataReceiver = Receiver<Bytes>;
type ExportChannelSender = flume::Sender<ExportDataReceiver>;
type ExportPartsReceiver = flume::Receiver<(ExportDataReceiver, OneShotServer<ExportService>)>;

/// An ETL EXPORT worker.
///
/// The type implements [`AsyncRead`] and is [`Send`] and [`Sync`] so it can be freely used
/// in any data pipeline.
///
/// # IMPORTANT
///
/// During `EXPORT` jobs Exasol will make a series of HTTP requests to the driver, one for each
/// worker. Unless it has already buffered the remaining request data, dropping a reader before it
/// returned *EOF* will result in the `EXPORT` query returning an error because the request was not
/// entirely read and responded to.
#[derive(Debug)]
pub struct ExaExport(MaybeCompressedReader);

impl AsyncRead for ExaExport {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
    fn poll_read_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &mut [io::IoSliceMut<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_read_vectored(cx, bufs)
    }
}
