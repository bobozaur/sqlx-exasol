use std::{
    io::Result as IoResult,
    pin::Pin,
    task::{Context, Poll},
};

#[cfg(feature = "compression")]
use async_compression::futures::bufread::GzipDecoder;
use futures_io::AsyncRead;
use pin_project::pin_project;

use super::reader::ExportReader;
use crate::connection::websocket::socket::ExaSocket;

/// Wrapper enum that handles the compression support for the [`ExportReader`].
/// It makes use of [`ExportBufReader`] because the [`GzipDecoder`] needs a type
/// implementing [`futures_io::AsyncBufRead`].
#[pin_project(project = ExaExportReaderProj)]
#[derive(Debug)]
pub enum ExaExportReader {
    Plain(#[pin] ExportReader),
    #[cfg(feature = "compression")]
    Compressed(#[pin] GzipDecoder<ExportReader>),
}

impl ExaExportReader {
    pub fn new(socket: ExaSocket, buffer_size: usize, with_compression: bool) -> Self {
        let reader = ExportReader::new(socket, buffer_size);

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

impl AsyncRead for ExaExportReader {
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
