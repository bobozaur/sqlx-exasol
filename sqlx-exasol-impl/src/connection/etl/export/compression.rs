use std::{
    fmt::Debug,
    io,
    pin::Pin,
    task::{Context, Poll},
};

#[cfg(feature = "compression")]
use async_compression::futures::bufread::GzipDecoder;
use futures_io::AsyncRead;

use super::reader::ExaReaderInner;
use crate::etl::export::ExportChannelReceiver;

/// Wrapper enum that handles the compression support for the [`ExaReader`].
#[derive(Debug)]
pub enum ExaReader {
    Plain(ExaReaderInner),
    #[cfg(feature = "compression")]
    Compressed(GzipDecoder<ExaReaderInner>),
}

impl ExaReader {
    #[allow(unused_variables, reason = "conditionally compiled")]
    pub fn new(rx: ExportChannelReceiver, with_compression: bool) -> Self {
        let reader = ExaReaderInner::new(rx);

        #[cfg(feature = "compression")]
        if with_compression {
            let mut reader = GzipDecoder::new(reader);
            reader.multiple_members(true);
            return Self::Compressed(reader);
        }

        Self::Plain(reader)
    }
}

impl AsyncRead for ExaReader {
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
