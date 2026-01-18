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

use compression::ExaReader;
use flume::{Receiver, Sender};
use futures_io::AsyncRead;
use hyper::body::Bytes;
pub use options::ExportBuilder;

type ExportDataSender = Sender<Bytes>;
type ExportDataReceiver = Receiver<Bytes>;
type ExportChannelSender = Sender<ExportDataReceiver>;
type ExportChannelReceiver = Receiver<ExportDataReceiver>;

/// An ETL EXPORT worker.
///
/// The type implements [`AsyncRead`] and is [`Send`] and [`Sync`] so it can be freely used
/// in any data pipeline.
///
/// # IMPORTANT
///
/// Dropping a reader before it returned EOF will result in the `EXPORT` query returning an error.
/// While not necessarily a problem if you're not interested in the whole export, there's no way to
/// circumvent that other than handling the error in code.
#[derive(Debug)]
pub struct ExaExport(ExaReader);

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
