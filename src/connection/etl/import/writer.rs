use std::{
    future::Future,
    io,
    pin::Pin,
    sync::Mutex,
    task::{ready, Context, Poll},
};

use futures_channel::mpsc::{Receiver, SendError, Sender};
use futures_io::{AsyncWrite, IoSlice};
use futures_util::{future::Fuse, FutureExt, SinkExt, Stream, StreamExt};
use http_body_util::{combinators::Collect, BodyExt, StreamBody};
use hyper::{
    body::{Bytes, Frame, Incoming},
    server::conn::http1::{Builder, Connection},
    service::Service,
    Request, Response, StatusCode,
};
use sqlx_core::bytes::BytesMut;

use crate::connection::websocket::socket::ExaSocket;

pub type ImportConnection = Connection<ExaSocket, ImportService>;
type ImportResponse = Response<StreamBody<ImportStream>>;

pub struct ImportStream(Receiver<BytesMut>);

impl Stream for ImportStream {
    type Item = Result<Frame<Bytes>, hyper::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let out = ready!(self.0.poll_next_unpin(cx))
            .map(From::from)
            .map(Frame::data)
            .map(Ok);

        Poll::Ready(out)
    }
}

pub struct ImportFuture {
    inner: Collect<Incoming>,
    stream: Option<ImportStream>,
}

impl ImportFuture {
    pub fn new(req: Request<Incoming>, rx: ImportStream) -> Self {
        Self {
            inner: req.into_body().collect(),
            stream: Some(rx),
        }
    }
}

impl Future for ImportFuture {
    type Output = io::Result<ImportResponse>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        ready!(self.inner.poll_unpin(cx)).map_err(map_hyper_err)?;

        let response = Response::builder()
            .status(StatusCode::OK)
            .body(StreamBody::new(self.stream.take().unwrap()))
            .map_err(map_http_error);

        Poll::Ready(response)
    }
}

pub struct ImportService(Mutex<Option<Receiver<BytesMut>>>);

impl ImportService {
    pub fn new(rx: Receiver<BytesMut>) -> Self {
        Self(Mutex::new(Some(rx)))
    }
}

impl Service<Request<Incoming>> for ImportService {
    type Response = ImportResponse;

    type Error = io::Error;

    type Future = Fuse<ImportFuture>;

    fn call(&self, req: Request<Incoming>) -> Self::Future {
        let rx = self.0.lock().unwrap().take().unwrap();
        ImportFuture::new(req, ImportStream(rx)).fuse()
    }
}

#[derive(Debug)]
pub struct ExaWriter {
    conn: ImportConnection,
    buffer: BytesMut,
    max_buf_size: usize,
    sink: Sender<BytesMut>,
}

impl ExaWriter {
    pub fn new(socket: ExaSocket, max_buf_size: usize) -> Self {
        let (data_tx, data_rx) = futures_channel::mpsc::channel(0);
        let service = ImportService::new(data_rx);
        let conn = Builder::new().serve_connection(socket, service);

        Self {
            conn,
            buffer: BytesMut::with_capacity(max_buf_size),
            max_buf_size,
            sink: data_tx,
        }
    }
}

impl ExaWriter {
    fn poll_write_internal(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.max_buf_size == self.buffer.len() {
            ready!(self.as_mut().poll_flush(cx))?;
        }

        let avail = self.max_buf_size - self.buffer.len();
        let len = buf.len().min(avail);
        self.buffer.extend_from_slice(&buf[..len]);
        Poll::Ready(Ok(len))
    }

    fn poll_write_vectored_internal(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        if self.max_buf_size == self.buffer.len() {
            ready!(self.as_mut().poll_flush(cx))?;
        }

        let avail = self.max_buf_size - self.buffer.len();
        let mut rem = avail;
        for buf in bufs {
            if rem == 0 {
                break;
            }

            let len = buf.len().min(rem);
            self.buffer.extend_from_slice(&buf[..len]);
            rem -= len;
        }

        Poll::Ready(Ok(avail - rem))
    }
}

impl AsyncWrite for ExaWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.poll_write_internal(cx, buf)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.poll_write_vectored_internal(cx, bufs)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let _ = self.as_mut().conn.poll_unpin(cx).map_err(map_hyper_err)?;

        if !self.buffer.is_empty() {
            ready!(self.sink.poll_ready_unpin(cx)).map_err(map_send_error)?;
            let new_buffer = BytesMut::with_capacity(self.max_buf_size);
            let buffer = std::mem::replace(&mut self.buffer, new_buffer);
            self.sink.start_send_unpin(buffer).map_err(map_send_error)?;
        }

        Poll::Ready(Ok(()))
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if !self.sink.is_closed() {
            ready!(self.as_mut().poll_flush(cx))?;
            ready!(self.sink.poll_close_unpin(cx)).map_err(map_send_error)?;
        }

        ready!(self.conn.poll_unpin(cx)).map_err(map_hyper_err)?;
        Poll::Ready(Ok(()))
    }
}

fn map_hyper_err(err: hyper::Error) -> io::Error {
    let kind = if err.is_timeout() {
        io::ErrorKind::TimedOut
    } else if err.is_parse_too_large() {
        io::ErrorKind::InvalidData
    } else {
        io::ErrorKind::BrokenPipe
    };

    io::Error::new(kind, err)
}

fn map_send_error(err: SendError) -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, err)
}

fn map_http_error(err: hyper::http::Error) -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, err)
}
