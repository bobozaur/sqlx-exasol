use std::{
    future::Future,
    pin::Pin,
    task::{ready, Context, Poll},
};

use async_compression::futures::write::{ZlibDecoder, ZlibEncoder};
use async_tungstenite::{tungstenite::Message, WebSocketStream};
use futures_core::Stream;
use futures_io::AsyncWrite;
use futures_util::{io::BufReader, FutureExt, Sink, SinkExt, StreamExt};
use sqlx_core::bytes::Bytes;

use crate::{
    connection::websocket::{socket::ExaSocket, transport::PlainWebSocket},
    error::{ExaProtocolError, ToSqlxError},
    SqlxError, SqlxResult,
};

#[derive(Debug)]
pub struct CompressedWebSocket {
    pub inner: WebSocketStream<BufReader<ExaSocket>>,
    decoding: Option<Compression<ZlibDecoder<Vec<u8>>>>,
    encoding: Option<Compression<ZlibEncoder<Vec<u8>>>>,
}

impl Stream for CompressedWebSocket {
    type Item = SqlxResult<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            // Decompress the last message, if any
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

            // The whole point of compression is to end up with smaller data so we might as well
            // allocate the length we know from the compressed data in advance.
            let capacity = bytes.len();
            self.decoding = Some(Compression::new(bytes, capacity));
        }
    }
}

impl Sink<String> for CompressedWebSocket {
    type Error = SqlxError;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner
            .poll_ready_unpin(cx)
            .map_err(ToSqlxError::to_sqlx_err)
    }

    fn start_send(mut self: Pin<&mut Self>, item: String) -> Result<(), Self::Error> {
        if self.encoding.is_some() {
            return Err(ExaProtocolError::SendNotReady)?;
        }

        let bytes = item.into_bytes().into_boxed_slice().into();
        self.encoding = Some(Compression::new(bytes, 0));
        Ok(())
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        loop {
            if let Some(future) = self.encoding.as_mut() {
                let bytes = ready!(future.poll_unpin(cx))?;
                self.encoding = None;
                self.inner
                    .start_send_unpin(Message::Binary(bytes))
                    .map_err(ToSqlxError::to_sqlx_err)?;
            } else {
                return self
                    .inner
                    .poll_flush_unpin(cx)
                    .map_err(ToSqlxError::to_sqlx_err);
            }
        }
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
    writer: T,
    offset: usize,
    data: Bytes,
    state: CompressionState,
}

impl<T> Compression<T>
where
    T: ExaCompression,
{
    fn new(data: Bytes, capacity: usize) -> Self {
        Self {
            writer: T::new(capacity),
            offset: 0,
            data,
            state: CompressionState::Writing,
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
            match this.state {
                CompressionState::Writing => {
                    let buf = &this.data[this.offset..];
                    let written = ready!(Pin::new(&mut this.writer).poll_write(cx, buf))?;
                    this.offset += written;

                    if written == 0 {
                        this.state = CompressionState::Flushing;
                    }
                }
                CompressionState::Flushing => {
                    ready!(Pin::new(&mut this.writer).poll_flush(cx))?;
                    this.state = CompressionState::Closing;
                }
                CompressionState::Closing => {
                    ready!(Pin::new(&mut this.writer).poll_close(cx))?;
                    return Poll::Ready(Ok(this.writer.take_buffer()));
                }
            }
        }
    }
}

#[derive(Debug)]
enum CompressionState {
    Writing,
    Flushing,
    Closing,
}

trait ExaCompression: AsyncWrite + Unpin {
    fn new(capacity: usize) -> Self;

    fn take_buffer(&mut self) -> Bytes;
}

impl ExaCompression for ZlibDecoder<Vec<u8>> {
    fn new(capacity: usize) -> Self {
        Self::new(Vec::with_capacity(capacity))
    }

    fn take_buffer(&mut self) -> Bytes {
        std::mem::take(self.get_mut()).into()
    }
}

impl ExaCompression for ZlibEncoder<Vec<u8>> {
    fn new(capacity: usize) -> Self {
        Self::new(Vec::with_capacity(capacity))
    }

    fn take_buffer(&mut self) -> Bytes {
        std::mem::take(self.get_mut()).into()
    }
}
