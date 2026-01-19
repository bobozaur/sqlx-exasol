use std::{
    fmt::Debug,
    io,
    pin::Pin,
    task::{Context, Poll},
};

#[cfg(feature = "compression")]
use async_compression::futures::write::GzipEncoder;
use futures_io::AsyncWrite;

use super::writer::ExaWriter;
use crate::etl::import::ImportChannelReceiver;

/// An [`AsyncWrite`] implementation for an `IMPORT` worker.
///
/// This enum wraps the underlying writer and handles compression if the `compression`
/// feature is enabled.
#[derive(Debug)]
pub enum MaybeCompressedWriter {
    Plain(ExaWriter),
    #[cfg(feature = "compression")]
    Compressed(GzipEncoder<ExaWriter>),
}

impl MaybeCompressedWriter {
    pub fn new(
        rx: ImportChannelReceiver,
        buffer_size: usize,
        #[allow(unused_variables, reason = "conditionally compiled")] with_compression: bool,
    ) -> Self {
        let writer = ExaWriter::new(rx, buffer_size);

        #[cfg(feature = "compression")]
        if with_compression {
            return Self::Compressed(GzipEncoder::new(writer));
        }

        Self::Plain(writer)
    }
}

impl AsyncWrite for MaybeCompressedWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            #[cfg(feature = "compression")]
            Self::Compressed(s) => Pin::new(s).poll_write(cx, buf),
            Self::Plain(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            #[cfg(feature = "compression")]
            Self::Compressed(s) => Pin::new(s).poll_write_vectored(cx, bufs),
            Self::Plain(s) => Pin::new(s).poll_write_vectored(cx, bufs),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            #[cfg(feature = "compression")]
            Self::Compressed(s) => Pin::new(s).poll_flush(cx),
            Self::Plain(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            #[cfg(feature = "compression")]
            Self::Compressed(s) => Pin::new(s).poll_close(cx),
            Self::Plain(s) => Pin::new(s).poll_close(cx),
        }
    }
}
