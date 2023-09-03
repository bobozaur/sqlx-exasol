mod compression;
mod options;
mod reader;

use std::{
    fmt::Debug,
    io::Result as IoResult,
    pin::Pin,
    task::{ready, Context, Poll},
};

use compression::ExaExportReader;
use futures_io::AsyncRead;
use futures_util::FutureExt;
pub use options::{ExportBuilder, QueryOrTable};
use pin_project::pin_project;

use super::SocketFuture;

#[allow(clippy::large_enum_variant)]
#[pin_project(project = ExaExportProj)]
pub enum ExaExport {
    Setup(#[pin] SocketFuture, bool),
    Reading(#[pin] ExaExportReader),
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
            let (socket, with_compression) = match self.as_mut().project() {
                ExaExportProj::Reading(r) => return r.poll_read(cx, buf),
                ExaExportProj::Setup(mut f, c) => (ready!(f.poll_unpin(cx))?, *c),
            };

            let reader = ExaExportReader::new(socket, with_compression);
            self.set(Self::Reading(reader));
        }
    }
}
