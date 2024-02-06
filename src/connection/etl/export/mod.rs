mod export_source;
mod options;
mod reader;

use std::{
    fmt::Debug,
    io::Result as IoResult,
    pin::Pin,
    task::{Context, Poll},
};

#[cfg(feature = "compression")]
use async_compression::futures::bufread::GzipDecoder;
pub use export_source::ExportSource;
use futures_io::AsyncRead;
pub use options::ExportBuilder;
use pin_project::pin_project;
use reader::ExaReader as ExportReader;

use crate::connection::websocket::socket::ExaSocket;

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

/// Wrapper enum that handles the compression support for the [`ExportReader`].
/// It makes use of [`ExportBufReader`] because the [`GzipDecoder`] needs a type
/// implementing [`futures_io::AsyncBufRead`].
#[pin_project(project = ExaExportReaderProj)]
#[derive(Debug)]
pub enum ExaExport {
    Plain(#[pin] ExportReader),
    #[cfg(feature = "compression")]
    Compressed(#[pin] GzipDecoder<ExportReader>),
}

impl ExaExport {
    pub fn new(socket: ExaSocket, with_compression: bool) -> Self {
        let reader = ExportReader::new(socket);

        match with_compression {
            #[cfg(feature = "compression")]
            true => {
                let mut reader = GzipDecoder::new(reader);
                reader.multiple_members(true);
                Self::Compressed(reader)
            }
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
            ExaExportReaderProj::Compressed(mut r) => r.as_mut().poll_read(cx, buf),
            ExaExportReaderProj::Plain(r) => r.poll_read(cx, buf),
        }
    }
}
