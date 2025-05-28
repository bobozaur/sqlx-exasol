mod compression;
mod export_source;
mod options;
mod reader;

use std::{
    fmt::Debug,
    io::Result as IoResult,
    pin::Pin,
    task::{ready, Context, Poll},
};

use compression::ExaExportReader;
pub use export_source::ExportSource;
use futures_io::AsyncRead;
use futures_util::FutureExt;
pub use options::ExportBuilder;

use super::SocketFuture;

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
#[allow(clippy::large_enum_variant)]
pub enum ExaExport {
    Setup(SocketFuture, bool),
    Reading(ExaExportReader),
}

impl Debug for ExaExport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Setup(..) => f.debug_tuple("Setup").finish(),
            Self::Reading(arg0) => f.debug_tuple("Reading").field(arg0).finish(),
        }
    }
}

impl AsyncRead for ExaExport {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<IoResult<usize>> {
        loop {
            let (socket, with_compression) = match self.as_mut().get_mut() {
                Self::Reading(r) => return Pin::new(r).poll_read(cx, buf),
                Self::Setup(f, c) => (ready!(f.poll_unpin(cx))?, *c),
            };

            let reader = ExaExportReader::new(socket, with_compression);
            self.set(Self::Reading(reader));
        }
    }
}
