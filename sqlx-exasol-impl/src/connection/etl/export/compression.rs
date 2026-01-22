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
use crate::etl::export::ExportPartsReceiver;

/// An [`AsyncRead`] implementation for an `EXPORT` worker.
///
/// This enum wraps the underlying reader and handles decompression if the `compression`
/// feature is enabled.
#[derive(Debug)]
pub enum MaybeCompressedReader {
    Plain(ExaReader),
    #[cfg(feature = "compression")]
    Compressed(GzipDecoder<ExaReader>),
}

impl MaybeCompressedReader {
    #[allow(unused_variables, reason = "conditionally compiled")]
    pub fn new(rx: ExportPartsReceiver, with_compression: bool) -> Self {
        let reader = ExaReader::new(rx);

        #[cfg(feature = "compression")]
        if with_compression {
            let mut reader = GzipDecoder::new(reader);
            reader.multiple_members(true);
            return Self::Compressed(reader);
        }

        Self::Plain(reader)
    }
}

impl AsyncRead for MaybeCompressedReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(r) => Pin::new(r).poll_read(cx, buf),
            #[cfg(feature = "compression")]
            Self::Compressed(r) => Pin::new(r).poll_read(cx, buf),
        }
    }

    fn poll_read_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &mut [io::IoSliceMut<'_>],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(r) => Pin::new(r).poll_read_vectored(cx, bufs),
            #[cfg(feature = "compression")]
            Self::Compressed(r) => Pin::new(r).poll_read_vectored(cx, bufs),
        }
    }
}
