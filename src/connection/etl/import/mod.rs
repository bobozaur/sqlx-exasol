use std::{
    io::Result as IoResult,
    pin::Pin,
    task::{Context, Poll},
};

use async_compression::futures::write::GzipEncoder;
use futures_io::AsyncWrite;
use pin_project::pin_project;

use self::writer::ImportWriter;

mod options;
mod writer;

pub use options::{ImportBuilder, Trim};

#[pin_project(project = ExaImportProj)]
#[derive(Debug)]
pub enum ExaImport {
    #[cfg(feature = "compression")]
    Compressed(#[pin] GzipEncoder<ImportWriter>),
    Plain(#[pin] ImportWriter),
}

impl ExaImport {
    pub(crate) fn new(writer: ImportWriter, compression: bool) -> Self {
        match compression {
            #[cfg(feature = "compression")]
            true => Self::Compressed(GzipEncoder::new(writer)),
            _ => Self::Plain(writer),
        }
    }
}

impl AsyncWrite for ExaImport {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<IoResult<usize>> {
        match self.project() {
            #[cfg(feature = "compression")]
            ExaImportProj::Compressed(s) => s.poll_write(cx, buf),
            ExaImportProj::Plain(s) => s.poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        match self.project() {
            #[cfg(feature = "compression")]
            ExaImportProj::Compressed(s) => s.poll_flush(cx),
            ExaImportProj::Plain(s) => s.poll_flush(cx),
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        match self.project() {
            #[cfg(feature = "compression")]
            ExaImportProj::Compressed(s) => s.poll_close(cx),
            ExaImportProj::Plain(s) => s.poll_close(cx),
        }
    }
}
