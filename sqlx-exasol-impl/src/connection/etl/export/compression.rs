use std::{
    fmt::Debug,
    io,
    pin::Pin,
    task::{Context, Poll},
};

#[cfg(feature = "compression")]
use async_compression::futures::bufread::GzipDecoder;
use futures_io::AsyncRead;

use super::reader::ExaReader;
use crate::connection::websocket::socket::ExaSocket;

/// Wrapper enum that handles the compression support for the [`ExaReader`].
#[derive(Debug)]
pub enum ExaExportReader {
    Plain(ExaReader),
    #[cfg(feature = "compression")]
    Compressed(GzipDecoder<ExaReader>),
}

impl ExaExportReader {
    #[allow(unused_variables, reason = "conditionally compiled")]
    pub fn new(socket: ExaSocket, with_compression: bool) -> Self {
        let reader = ExaReader::new(socket);

        #[cfg(feature = "compression")]
        if with_compression {
            let mut reader = GzipDecoder::new(reader);
            reader.multiple_members(true);
            return Self::Compressed(reader);
        }

        Self::Plain(reader)
    }
}

impl AsyncRead for ExaExportReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            #[cfg(feature = "compression")]
            Self::Compressed(r) => Pin::new(r).poll_read(cx, buf),
            Self::Plain(r) => Pin::new(r).poll_read(cx, buf),
        }
    }
}
