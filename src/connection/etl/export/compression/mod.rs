#[cfg(feature = "compression")]
mod buf_reader;

use std::{
    io::Result as IoResult,
    pin::Pin,
    task::{Context, Poll},
};

#[cfg(feature = "compression")]
use async_compression::futures::bufread::GzipDecoder;
#[cfg(feature = "compression")]
use buf_reader::ExportBufReader;
use futures_io::AsyncRead;
use pin_project::pin_project;

use super::reader::ExportReader;
use crate::connection::websocket::socket::ExaSocket;

#[pin_project(project = ExaExportReaderProj)]
#[derive(Debug)]
pub enum ExaExportReader {
    Plain(#[pin] ExportReader),
    #[cfg(feature = "compression")]
    Compressed(#[pin] GzipDecoder<ExportBufReader>),
}

impl ExaExportReader {
    pub fn new(socket: ExaSocket, with_compression: bool) -> Self {
        let reader = ExportReader::new(socket);

        match with_compression {
            #[cfg(feature = "compression")]
            true => Self::Compressed(GzipDecoder::new(ExportBufReader::new(reader))),
            _ => Self::Plain(reader),
        }
    }
}

impl AsyncRead for ExaExportReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<IoResult<usize>> {
        match self.as_mut().project() {
            #[cfg(feature = "compression")]
            ExaExportReaderProj::Compressed(r) => r.poll_read(cx, buf),
            ExaExportReaderProj::Plain(r) => r.poll_read(cx, buf),
        }
    }
}
