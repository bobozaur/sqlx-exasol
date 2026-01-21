use std::{
    future::Future,
    io,
    pin::Pin,
    task::{ready, Context, Poll},
};

use flume::r#async::SendFut;
use futures_util::{stream::Forward, FutureExt, Sink, SinkExt, Stream, StreamExt};
use hyper::{
    body::{Body, Bytes, Incoming},
    service::Service,
    Request, Response, StatusCode,
};

use crate::etl::{
    error::{map_http_error, map_hyper_err, server_bootstrap_error},
    export::{ExportChannelSender, ExportDataReceiver, ExportDataSender},
};

/// A [`hyper`] service that handles an `EXPORT` request from Exasol.
///
/// When an `EXPORT` job is started, Exasol makes an HTTP `POST` request to the
/// one-shot server. This service takes the body of that request and streams it
/// to the [`crate::connection::etl::ExaExport`] worker.
pub struct ExportService(ExportChannelSender);

impl ExportService {
    pub fn new(tx: ExportChannelSender) -> Self {
        Self(tx)
    }
}

impl Service<Request<Incoming>> for ExportService {
    type Response = Response<String>;

    type Error = io::Error;

    type Future = ExportFuture;

    /// Once a request is received, create the data channel between the HTTP server
    /// and the [`crate::connection::etl::ExaExport`] and send the receiver end.
    ///
    /// We can then use the sender to pipe data through.
    fn call(&self, req: Request<Incoming>) -> Self::Future {
        let (tx, rx) = futures_channel::mpsc::channel(0);
        let send_fut = self.0.clone().into_send_async(rx);
        let sink = ExportSink::SendChan(send_fut, Some(tx));
        ExportFuture(ExportStream(req.into_body()).forward(sink))
    }
}

/// The future returned by [`ExportService`].
///
/// This future drives the process of streaming the request body to the worker.
/// Once the body has been fully streamed, it returns an empty `200 OK` response
/// to Exasol.
pub struct ExportFuture(Forward<ExportStream, ExportSink>);

impl Future for ExportFuture {
    type Output = io::Result<Response<String>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        ready!(self.0.poll_unpin(cx))?;

        let response = Response::builder()
            .status(StatusCode::OK)
            .body(String::new())
            .map_err(map_http_error);

        Poll::Ready(response)
    }
}

/// A stream that wraps the [`hyper`] request body (`Incoming`).
///
/// This stream yields chunks of the request body as `io::Result<Bytes>`.
struct ExportStream(Incoming);

impl Stream for ExportStream {
    type Item = io::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match ready!(Pin::new(&mut self.0).poll_frame(cx)) {
            Some(Ok(f)) => Poll::Ready(f.into_data().ok().map(Ok)),
            Some(Err(e)) => Poll::Ready(Some(Err(map_hyper_err(e)))),
            None => Poll::Ready(None),
        }
    }
}

/// A sink that forwards data to the [`crate::connection::etl::ExaExport`] worker.
///
/// This enum represents the two states of the sink:
/// 1. [`ExportSink::SendChan`]: The sink has not yet sent the data channel to the worker. The
///    channel will be sent after the HTTP request is received and processing starts.
/// 2. [`ExportSink::SendData`]: The channel has been sent, and the sink is now forwarding HTTP
///    request body data chunks to the worker.
pub enum ExportSink {
    SendChan(
        SendFut<'static, ExportDataReceiver>,
        Option<ExportDataSender>,
    ),
    SendData(ExportDataSender),
}

impl Sink<Bytes> for ExportSink {
    type Error = io::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        loop {
            let state = match self.as_mut().get_mut() {
                ExportSink::SendChan(send_fut, sender) => {
                    ready!(send_fut.poll_unpin(cx)).map_err(server_bootstrap_error)?;
                    ExportSink::SendData(sender.take().unwrap())
                }
                ExportSink::SendData(sink) => {
                    return sink.poll_ready_unpin(cx).map_err(server_send_data_error)
                }
            };

            self.set(state);
        }
    }

    fn start_send(self: Pin<&mut Self>, item: Bytes) -> Result<(), Self::Error> {
        match self.get_mut() {
            ExportSink::SendChan(..) => {
                unreachable!("start_send is always preceded by poll_ready")
            }
            ExportSink::SendData(sink) => {
                sink.start_send_unpin(item).map_err(server_send_data_error)
            }
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        loop {
            let state = match self.as_mut().get_mut() {
                ExportSink::SendChan(send_fut, sender) => {
                    ready!(send_fut.poll_unpin(cx)).map_err(server_bootstrap_error)?;
                    ExportSink::SendData(sender.take().unwrap())
                }
                ExportSink::SendData(sink) => {
                    return sink.poll_flush_unpin(cx).map_err(server_send_data_error)
                }
            };

            self.set(state);
        }
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        loop {
            let state = match self.as_mut().get_mut() {
                ExportSink::SendChan(send_fut, sender) => {
                    ready!(send_fut.poll_unpin(cx)).map_err(server_bootstrap_error)?;
                    ExportSink::SendData(sender.take().unwrap())
                }
                ExportSink::SendData(sink) => {
                    return sink.poll_close_unpin(cx).map_err(server_send_data_error)
                }
            };

            self.set(state);
        }
    }
}

fn server_send_data_error<T>(_: T) -> io::Error {
    io::Error::new(
        io::ErrorKind::BrokenPipe,
        "error sending data to the EXPORT worker",
    )
}
