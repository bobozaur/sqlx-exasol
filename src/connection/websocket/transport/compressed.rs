use std::{
    io::{BufRead, Read, Write},
    pin::Pin,
    task::{ready, Context, Poll},
};

use async_compression::futures::{bufread::ZlibDecoder, write::ZlibEncoder};
use async_tungstenite::{tungstenite::Message, WebSocketStream};
use bytes::{
    buf::{Reader, Writer},
    Buf, BufMut, Bytes, BytesMut,
};
use futures_core::Stream;
use futures_io::{AsyncBufRead, AsyncRead, AsyncWrite};
use futures_util::{io::BufReader, Sink, SinkExt, StreamExt, TryStreamExt};
use serde::de::DeserializeOwned;
use sqlx_core::Error as SqlxError;

use crate::{
    connection::websocket::{socket::ExaSocket, transport::PlainWebSocket},
    error::{ExaProtocolError, ToSqlxError},
    responses::{Attributes, Response},
};

#[derive(Debug)]
pub struct CompressedWebSocket {
    pub inner: WebSocketStream<BufReader<ExaSocket>>,
    decoding: Option<(BytesMut, usize, ZlibDecoder<AsyncReader>)>,
    encoding: Option<(String, usize, ZlibEncoder<AsyncWriter>)>,
}

impl CompressedWebSocket {
    /// Receives a compressed [`Response<T>`] and decompresses it.
    pub async fn recv<T>(&mut self) -> Result<(T, Option<Attributes>), SqlxError>
    where
        T: DeserializeOwned,
    {
        let Some(bytes) = self.try_next().await? else {
            return Err(ExaProtocolError::from(None))?;
        };

        let res: Result<Response<_>, _> = serde_json::from_slice(&bytes);
        let response = res.map_err(ToSqlxError::to_sqlx_err)?;
        Result::from(response).map_err(From::from)
    }
}

impl Stream for CompressedWebSocket {
    type Item = Result<Bytes, SqlxError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            // Decrypt the last message, if any
            match self.decoding.as_mut() {
                Some((buf, offset, decoder)) => {
                    let read = ready!(Pin::new(decoder).poll_read(cx, &mut buf[*offset..]))?;
                    *offset += read;

                    // If the reader ended, we can return the decrypted data.
                    if read == 0 {
                        let buf = std::mem::take(buf);
                        self.decoding = None;
                        return Poll::Ready(Some(Ok(buf.into())));
                    }
                }
                None => {
                    // Get a new message
                    let Some(msg) = ready!(self.inner.poll_next_unpin(cx)) else {
                        return Poll::Ready(None);
                    };

                    let bytes = match msg.map_err(ToSqlxError::to_sqlx_err)? {
                        Message::Text(s) => s.into(),
                        Message::Binary(v) => v,
                        Message::Close(c) => Err(ExaProtocolError::from(c))?,
                        // Ignore other messages and pend for the next
                        _ => return Poll::Pending,
                    };

                    self.decoding = Some((
                        BytesMut::with_capacity(bytes.len()),
                        0,
                        ZlibDecoder::new(AsyncReader(bytes.reader())),
                    ))
                }
            }
        }
    }
}

impl Sink<String> for CompressedWebSocket {
    type Error = SqlxError;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        loop {
            match self.encoding.as_mut() {
                Some((buf, offset, encoder)) => {
                    let buf = buf.as_bytes();
                    let mut encoder = Pin::new(encoder);
                    let written = ready!(encoder.as_mut().poll_write(cx, &buf[*offset..]))?;
                    *offset += written;

                    if *offset >= buf.len() {
                        let bytes = std::mem::take(encoder.get_mut().get_mut().0.get_mut());
                        self.encoding = None;
                        self.inner
                            .start_send_unpin(Message::Binary(bytes.into()))
                            .map_err(ToSqlxError::to_sqlx_err)?;
                    }
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
        match self.encoding {
            Some(_) => Err(ExaProtocolError::SendNotReady)?,
            None => {
                let encoder = ZlibEncoder::new(AsyncWriter(BytesMut::new().writer()));
                self.encoding = Some((item, 0, encoder));
                Ok(())
            }
        }
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

#[derive(Debug)]
struct AsyncWriter(Writer<BytesMut>);

impl AsyncWrite for AsyncWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Poll::Ready(self.0.write(buf))
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        Poll::Ready(self.0.write_vectored(bufs))
    }

    fn poll_flush(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(self.0.flush())
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.poll_flush(cx)
    }
}
