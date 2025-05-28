use std::{
    future::Future,
    io::{BufRead, Read},
    pin::Pin,
    task::{ready, Context, Poll},
};

use async_compression::futures::bufread::{ZlibDecoder, ZlibEncoder};
use async_tungstenite::{tungstenite::Message, WebSocketStream};
use futures_core::Stream;
use futures_io::{AsyncBufRead, AsyncRead};
use futures_util::{io::BufReader, FutureExt, Sink, SinkExt, StreamExt};
use sqlx_core::{
    bytes::{buf::Reader, Buf, Bytes, BytesMut},
    Error as SqlxError,
};

use crate::{
    connection::websocket::{socket::ExaSocket, transport::PlainWebSocket},
    error::{ExaProtocolError, ToSqlxError},
};

#[derive(Debug)]
pub struct CompressedWebSocket {
    pub inner: WebSocketStream<BufReader<ExaSocket>>,
    decoding: Option<Compression<ZlibDecoder<AsyncReader>>>,
    encoding: Option<Compression<ZlibEncoder<AsyncReader>>>,
}

impl Stream for CompressedWebSocket {
    type Item = Result<Bytes, SqlxError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            // Decrypt the last message, if any
            if let Some(future) = self.decoding.as_mut() {
                let bytes = ready!(future.poll_unpin(cx))?;
                self.decoding = None;
                return Poll::Ready(Some(Ok(bytes)));
            }

            // Get a new message
            let Some(msg) = ready!(self.inner.poll_next_unpin(cx)) else {
                return Poll::Ready(None);
            };

            let bytes = match msg.map_err(ToSqlxError::to_sqlx_err)? {
                Message::Text(s) => s.into(),
                Message::Binary(v) => v,
                Message::Close(c) => Err(ExaProtocolError::from(c))?,
                // Ignore other messages and wait for the next
                _ => continue,
            };

            self.decoding = Some(Compression::new(bytes));
        }
    }
}

impl Sink<String> for CompressedWebSocket {
    type Error = SqlxError;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        loop {
            match self.encoding.as_mut() {
                Some(future) => {
                    let bytes = ready!(future.poll_unpin(cx))?;
                    self.encoding = None;
                    self.inner
                        .start_send_unpin(Message::Binary(bytes))
                        .map_err(ToSqlxError::to_sqlx_err)?;
                }
                None => {
                    return self
                        .inner
                        .poll_ready_unpin(cx)
                        .map_err(ToSqlxError::to_sqlx_err)
                }
            }
        }
    }

    fn start_send(mut self: Pin<&mut Self>, item: String) -> Result<(), Self::Error> {
        if self.encoding.is_some() {
            return Err(ExaProtocolError::SendNotReady)?;
        }

        let bytes = item.into_bytes().into_boxed_slice().into();
        self.encoding = Some(Compression::new(bytes));
        Ok(())
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner
            .poll_flush_unpin(cx)
            .map_err(ToSqlxError::to_sqlx_err)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner
            .poll_close_unpin(cx)
            .map_err(ToSqlxError::to_sqlx_err)
    }
}

impl From<PlainWebSocket> for CompressedWebSocket {
    fn from(value: PlainWebSocket) -> Self {
        Self {
            inner: value.0,
            decoding: None,
            encoding: None,
        }
    }
}

#[derive(Debug)]
struct Compression<T> {
    reader: T,
    offset: usize,
    buffer: BytesMut,
}

impl<T> Compression<T>
where
    T: ExaCompression,
{
    fn new(bytes: Bytes) -> Self {
        Self {
            reader: AsyncReader(bytes.reader()).into(),
            offset: 0,
            buffer: BytesMut::new(),
        }
    }
}

impl<T> Future for Compression<T>
where
    T: ExaCompression,
{
    type Output = std::io::Result<Bytes>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            let buf = &mut this.buffer[this.offset..];
            let written = ready!(Pin::new(&mut this.reader).poll_read(cx, buf))?;
            this.offset += written;

            if this.offset >= this.buffer.len() {
                return Poll::Ready(Ok(this.reader.take_buffer()));
            }
        }
    }
}

trait ExaCompression: From<AsyncReader> + AsyncRead + Unpin {
    fn take_buffer(&mut self) -> Bytes;
}

impl ExaCompression for ZlibDecoder<AsyncReader> {
    fn take_buffer(&mut self) -> Bytes {
        std::mem::take(self.get_mut().0.get_mut())
    }
}

impl ExaCompression for ZlibEncoder<AsyncReader> {
    fn take_buffer(&mut self) -> Bytes {
        std::mem::take(self.get_mut().0.get_mut())
    }
}

impl From<AsyncReader> for ZlibEncoder<AsyncReader> {
    fn from(value: AsyncReader) -> Self {
        Self::new(value)
    }
}

impl From<AsyncReader> for ZlibDecoder<AsyncReader> {
    fn from(value: AsyncReader) -> Self {
        Self::new(value)
    }
}

#[derive(Debug)]
struct AsyncReader(Reader<Bytes>);

impl AsyncRead for AsyncReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        Poll::Ready(self.0.read(buf))
    }

    fn poll_read_vectored(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bufs: &mut [std::io::IoSliceMut<'_>],
    ) -> Poll<std::io::Result<usize>> {
        Poll::Ready(self.0.read_vectored(bufs))
    }
}

impl AsyncBufRead for AsyncReader {
    fn poll_fill_buf(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<&[u8]>> {
        Poll::Ready(self.get_mut().0.fill_buf())
    }

    fn consume(mut self: Pin<&mut Self>, amt: usize) {
        self.0.consume(amt);
    }
}
