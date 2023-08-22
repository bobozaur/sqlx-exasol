use std::{
    io::Result as IoResult,
    pin::Pin,
    task::{Context, Poll},
};

use async_compression::futures::bufread::GzipDecoder;
use futures_io::AsyncRead;
use pin_project::pin_project;

use self::reader::ExportReader;

mod options;
mod reader;

pub use options::{ExportOptions, QueryOrTable};

#[pin_project(project = ExaExportProj)]
#[derive(Debug)]
pub enum ExaExport {
    #[cfg(feature = "compression")]
    Compressed(#[pin] GzipDecoder<ExportReader>),
    Plain(#[pin] ExportReader),
}

impl ExaExport {
    pub(crate) fn new(reader: ExportReader, compression: bool) -> Self {
        match compression {
            #[cfg(feature = "compression")]
            true => Self::Compressed(GzipDecoder::new(reader)),
            _ => Self::Plain(reader),
        }
    }
}

impl AsyncRead for ExaExport {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<IoResult<usize>> {
        match self.project() {
            #[cfg(feature = "compression")]
            ExaExportProj::Compressed(r) => r.poll_read(cx, buf),
            ExaExportProj::Plain(r) => r.poll_read(cx, buf),
        }
    }
}
