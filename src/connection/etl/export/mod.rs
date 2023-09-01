#[cfg(feature = "compression")]
mod buf_reader;
mod options;
mod reader;

use std::{
    io::Result as IoResult,
    pin::Pin,
    task::{ready, Context, Poll},
};

#[cfg(feature = "compression")]
use async_compression::futures::bufread::GzipDecoder;
use futures_core::future::BoxFuture;
use futures_io::AsyncRead;
use futures_util::FutureExt;
use pin_project::pin_project;

#[cfg(feature = "compression")]
use buf_reader::ExportBufReader;
pub use options::{ExportBuilder, QueryOrTable};
use reader::ExportReader;

use crate::connection::websocket::socket::ExaSocket;

#[pin_project(project = ExaExportProj)]
pub enum ExaExport {
    Setup(#[pin] BoxFuture<'static, IoResult<ExaSocket>>, bool),
    Plain(#[pin] ExportReader),
    #[cfg(feature = "compression")]
    Compressed(#[pin] GzipDecoder<ExportBufReader>),
}

impl ExaExport {
    fn make_reader(socket: ExaSocket, with_compression: bool) -> Self {
        let reader = ExportReader::new(socket);

        match with_compression {
            #[cfg(feature = "compression")]
            true => Self::Compressed(GzipDecoder::new(ExportBufReader::new(reader))),
            _ => Self::Plain(reader),
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
                #[cfg(feature = "compression")]
                ExaExportProj::Compressed(r) => return r.poll_read(cx, buf),
                ExaExportProj::Plain(r) => return r.poll_read(cx, buf),
                ExaExportProj::Setup(mut f, c) => (ready!(f.poll_unpin(cx))?, *c),
            };

            self.set(Self::make_reader(socket, with_compression))
        }
    }
}
