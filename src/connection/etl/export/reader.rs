use std::{
    future::Future,
    io::{Error as IoError, ErrorKind as IoErrorKind, Result as IoResult},
    pin::{pin, Pin},
    sync::Mutex,
    task::{ready, Context, Poll},
};

use futures_channel::mpsc::{Receiver, SendError, Sender};
use futures_core::FusedFuture;
use futures_io::{AsyncBufRead, AsyncRead};
use futures_util::{
    future::Fuse,
    stream::{Forward, IntoAsyncRead},
    FutureExt, Sink, SinkExt, Stream, StreamExt, TryStreamExt,
};
use http_body_util::Empty;
use hyper::{
    body::{Body, Bytes, Incoming},
    server::conn::http1::{Builder, Connection},
    service::Service,
    Request, Response, StatusCode,
};
use pin_project::pin_project;

use crate::connection::websocket::socket::ExaSocket;

type ExportResponse = Response<Empty<&'static [u8]>>;
pub type ExportConnection = Fuse<Connection<ExaSocket, ExportService>>;
pub type ExportReader = IntoAsyncRead<ReaderStream>;

#[pin_project]
struct ExportStream(#[pin] Incoming);

impl Stream for ExportStream {
    type Item = IoResult<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match ready!(self.project().0.poll_frame(cx)) {
            Some(Ok(f)) => Poll::Ready(f.into_data().ok().map(Ok)),
            Some(Err(e)) => Poll::Ready(Some(Err(map_hyper_err(e)))),
            None => Poll::Ready(None),
        }
    }
}

pub struct ExportSink(Sender<Bytes>);

impl Sink<Bytes> for ExportSink {
    type Error = IoError;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.0.poll_ready_unpin(cx).map_err(map_send_error)
    }

    fn start_send(mut self: Pin<&mut Self>, item: Bytes) -> Result<(), Self::Error> {
        self.0.start_send_unpin(item).map_err(map_send_error)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.0.poll_flush_unpin(cx).map_err(map_send_error)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.0.poll_close_unpin(cx).map_err(map_send_error)
    }
}

pub struct ExportFuture(Forward<ExportStream, ExportSink>);

impl Future for ExportFuture {
    type Output = IoResult<ExportResponse>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match ready!(self.0.poll_unpin(cx)) {
            Ok(()) => (),
            Err(e) => return Poll::Ready(Err(e)),
        };

        let response = Response::builder()
            .status(StatusCode::OK)
            .body(Empty::new())
            .map_err(map_http_error);

        Poll::Ready(response)
    }
}

pub struct ExportService(Mutex<Option<ExportSink>>);

impl ExportService {
    pub fn new(sink: ExportSink) -> Self {
        Self(Mutex::new(Some(sink)))
    }
}

impl Service<Request<Incoming>> for ExportService {
    type Response = ExportResponse;

    type Error = IoError;

    type Future = ExportFuture;

    fn call(&self, req: Request<Incoming>) -> Self::Future {
        let sink = self.0.lock().unwrap().take().unwrap();
        ExportFuture(ExportStream(req.into_body()).forward(sink))
    }
}

#[derive(Debug)]
pub struct ReaderStream(Receiver<Bytes>);

impl Stream for ReaderStream {
    type Item = IoResult<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match ready!(self.0.poll_next_unpin(cx)) {
            Some(b) => Poll::Ready(Some(Ok(b))),
            None => Poll::Ready(None),
        }
    }
}

#[derive(Debug)]
#[pin_project]
pub struct ExaReader {
    #[pin]
    reader: ExportReader,
    conn: ExportConnection,
    state: ExaReaderState,
}

impl ExaReader {
    pub fn new(socket: ExaSocket) -> Self {
        let (data_tx, data_rx): (_, Receiver<Bytes>) = futures_channel::mpsc::channel(0);
        let service = ExportService::new(ExportSink(data_tx));
        let reader = ReaderStream(data_rx).into_async_read();
        let conn = Builder::new().serve_connection(socket, service).fuse();

        Self {
            reader,
            conn,
            state: ExaReaderState::Reading,
        }
    }

    fn poll_conn(&mut self, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        if !self.conn.is_terminated() {
            ready!(self.conn.poll_unpin(cx)).map_err(map_hyper_err)?;
        }

        self.state = ExaReaderState::Done;
        Poll::Ready(Ok(()))
    }

    fn poll_read_internal(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<IoResult<usize>> {
        if !self.conn.is_terminated() {
            let _ = self.as_mut().conn.poll_unpin(cx).map_err(map_hyper_err)?;
        }

        let res = ready!(self.as_mut().project().reader.poll_read(cx, buf));

        match res {
            Ok(0) => {
                self.state = ExaReaderState::Responding;
                self.poll_conn(cx).map_ok(|()| 0)
            }
            Ok(n) => Poll::Ready(Ok(n)),
            Err(e) => {
                self.state = ExaReaderState::Done;
                Poll::Ready(Err(e))
            }
        }
    }

    fn poll_read_vectored_internal(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &mut [std::io::IoSliceMut<'_>],
    ) -> Poll<IoResult<usize>> {
        if !self.conn.is_terminated() {
            let _ = self.as_mut().conn.poll_unpin(cx).map_err(map_hyper_err)?;
        }

        let res = ready!(self.as_mut().project().reader.poll_read_vectored(cx, bufs));

        match res {
            Ok(0) => {
                self.state = ExaReaderState::Responding;
                self.poll_conn(cx).map_ok(|()| 0)
            }
            Ok(n) => Poll::Ready(Ok(n)),
            Err(e) => {
                self.state = ExaReaderState::Done;
                Poll::Ready(Err(e))
            }
        }
    }

    fn poll_fill_buf_internal<'a>(
        mut self: Pin<&'a mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<IoResult<&'a [u8]>> {
        if !self.conn.is_terminated() {
            let _ = self.as_mut().conn.poll_unpin(cx).map_err(map_hyper_err)?;
        }

        let this = self.project();
        let res = ready!(this.reader.poll_fill_buf(cx));

        match res {
            Ok(&[]) => {
                *this.state = ExaReaderState::Responding;
                ready!(this.conn.poll_unpin(cx)).map_err(map_hyper_err)?;
                *this.state = ExaReaderState::Done;
                Poll::Ready(Ok(&[]))
            }
            Ok(buf) => Poll::Ready(Ok(buf)),
            Err(e) => {
                *this.state = ExaReaderState::Done;
                Poll::Ready(Err(e))
            }
        }
    }
}

#[derive(Debug)]
enum ExaReaderState {
    Reading,
    Responding,
    Done,
}

impl AsyncRead for ExaReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<IoResult<usize>> {
        match self.state {
            ExaReaderState::Reading => self.poll_read_internal(cx, buf),
            ExaReaderState::Responding => self.poll_conn(cx).map_ok(|()| 0),
            ExaReaderState::Done => Poll::Ready(Ok(0)),
        }
    }

    fn poll_read_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &mut [std::io::IoSliceMut<'_>],
    ) -> Poll<IoResult<usize>> {
        match self.state {
            ExaReaderState::Reading => self.poll_read_vectored_internal(cx, bufs),
            ExaReaderState::Responding => self.poll_conn(cx).map_ok(|()| 0),
            ExaReaderState::Done => Poll::Ready(Ok(0)),
        }
    }
}

impl AsyncBufRead for ExaReader {
    fn poll_fill_buf(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<&[u8]>> {
        match self.state {
            ExaReaderState::Reading => self.poll_fill_buf_internal(cx),
            ExaReaderState::Responding => self.poll_conn(cx).map_ok(|()| [].as_slice()),
            ExaReaderState::Done => Poll::Ready(Ok(&[])),
        }
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        self.project().reader.consume(amt);
    }
}

fn map_hyper_err(err: hyper::Error) -> IoError {
    let kind = if err.is_timeout() {
        IoErrorKind::TimedOut
    } else if err.is_parse_too_large() {
        IoErrorKind::InvalidData
    } else {
        IoErrorKind::BrokenPipe
    };

    IoError::new(kind, err)
}

fn map_send_error(err: SendError) -> IoError {
    IoError::new(IoErrorKind::BrokenPipe, err)
}

fn map_http_error(err: hyper::http::Error) -> IoError {
    IoError::new(IoErrorKind::BrokenPipe, err)
}
