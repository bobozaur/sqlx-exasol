use std::{
    pin::Pin,
    task::{ready, Context, Poll},
};

use async_tungstenite::{
    tungstenite::{Bytes, Message},
    WebSocketStream,
};
use futures_core::Stream;
use futures_util::{io::BufReader, Sink, SinkExt, StreamExt};

use crate::{
    connection::websocket::socket::ExaSocket,
    error::{ExaProtocolError, ToSqlxError},
    SqlxError, SqlxResult,
};

/// A plain websocket that does not use compression.
#[derive(Debug)]
pub struct PlainWebSocket(pub WebSocketStream<BufReader<ExaSocket>>);

impl Stream for PlainWebSocket {
    type Item = SqlxResult<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            return match ready!(self.0.poll_next_unpin(cx))
                .transpose()
                .map_err(ToSqlxError::to_sqlx_err)?
            {
                Some(Message::Text(t)) => Poll::Ready(Some(Ok(t.into()))),
                Some(Message::Binary(b)) => Poll::Ready(Some(Ok(b))),
                Some(Message::Close(c)) => Err(ExaProtocolError::from(c))?,
                None => Poll::Ready(None),
                // Ignore other messages and wait for the next
                _ => continue,
            };
        }
    }
}

impl Sink<String> for PlainWebSocket {
    type Error = SqlxError;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.0
            .poll_ready_unpin(cx)
            .map_err(ToSqlxError::to_sqlx_err)
    }

    fn start_send(mut self: Pin<&mut Self>, item: String) -> Result<(), Self::Error> {
        self.0
            .start_send_unpin(Message::Text(item.into()))
            .map_err(ToSqlxError::to_sqlx_err)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.0
            .poll_flush_unpin(cx)
            .map_err(ToSqlxError::to_sqlx_err)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.0
            .poll_close_unpin(cx)
            .map_err(ToSqlxError::to_sqlx_err)
    }
}
