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
use sqlx_core::Error as SqlxError;

use crate::{
    connection::websocket::socket::ExaSocket,
    error::{ExaProtocolError, ToSqlxError},
};

#[derive(Debug)]
pub struct PlainWebSocket(pub WebSocketStream<BufReader<ExaSocket>>);

impl Stream for PlainWebSocket {
    type Item = Result<Bytes, SqlxError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let Some(msg) = ready!(self.0.poll_next_unpin(cx)) else {
                return Poll::Ready(None);
            };

            let bytes = match msg.map_err(ToSqlxError::to_sqlx_err)? {
                Message::Text(s) => s.into(),
                Message::Binary(v) => v,
                Message::Close(c) => Err(ExaProtocolError::from(c))?,
                // Ignore other messages and wait for the next
                _ => continue,
            };

            return Poll::Ready(Some(Ok(bytes)));
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
