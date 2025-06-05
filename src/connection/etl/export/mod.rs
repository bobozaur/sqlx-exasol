mod compression;
mod options;
mod reader;

use std::{
    fmt::Debug,
    io,
    pin::Pin,
    task::{ready, Context, Poll},
};

use compression::ExaExportReader;
use futures_io::AsyncRead;
use futures_util::FutureExt;
pub use options::ExportBuilder;

use super::WithSocketFuture;

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
pub struct ExaExport(ExaExportState);

impl AsyncRead for ExaExport {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        loop {
            let (socket, with_compression) = match &mut self.0 {
                ExaExportState::Poll(r) => return Pin::new(r).poll_read(cx, buf),
                ExaExportState::Handshake(f, c) => (ready!(f.poll_unpin(cx))?, *c),
            };

            let reader = ExaExportReader::new(socket, with_compression);
            self.set(Self(ExaExportState::Poll(reader)));
        }
    }
}

/// The EXPORT source type, which can either directly be a table or an entire query.
#[derive(Clone, Copy, Debug)]
pub enum ExportSource<'a> {
    Query(&'a str),
    Table(&'a str),
}

pub enum ExaExportState {
    /// TLS handshake is being performed.
    ///
    /// This approach is needed because Exasol will issue connections sequentially and thus perform
    /// TLS handshakes the same way.
    ///
    /// Therefore we accommodate the worker state until the query gets executed and data gets sent
    /// through the workers, which happens within consumer code.
    Handshake(WithSocketFuture, bool),
    /// The worker is fully connected and ready for I/O.
    Poll(ExaExportReader),
}

impl Debug for ExaExportState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Poll(arg0) => f.debug_tuple("Poll").field(arg0).finish(),
            Self::Handshake(..) => f.debug_tuple("Handshake").finish(),
        }
    }
}
