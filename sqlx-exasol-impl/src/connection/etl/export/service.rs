use std::{
    future::Future,
    io,
    pin::Pin,
    task::{ready, Context, Poll},
};

use flume::r#async::{SendFut, SendSink};
use futures_util::{stream::Forward, FutureExt, Sink, SinkExt, Stream, StreamExt};
use http_body_util::Empty;
use hyper::{
    body::{Body, Bytes, Incoming},
    service::Service,
    Request, Response, StatusCode,
};

use crate::etl::{
    error::{channel_pipe_error, data_pipe_error, map_http_error, map_hyper_err},
    export::{ExportChannelSender, ExportDataReceiver, ExportDataSender},
};

type ExportResponse = Response<Empty<&'static [u8]>>;

pub struct ExportService(ExportChannelSender);

impl ExportService {
    pub fn new(tx: ExportChannelSender) -> Self {
        Self(tx)
    }
}

impl Service<Request<Incoming>> for ExportService {
    type Response = ExportResponse;

    type Error = io::Error;

    type Future = ExportFuture;

    fn call(&self, req: Request<Incoming>) -> Self::Future {
        let (tx, rx) = flume::bounded(0);
        let send_fut = self.0.clone().into_send_async(rx);
        let sink = ExportSink::RecvChan(send_fut, Some(tx));
        ExportFuture(ExportStream(req.into_body()).forward(sink))
    }
}

pub struct ExportFuture(Forward<ExportStream, ExportSink>);

impl Future for ExportFuture {
    type Output = io::Result<ExportResponse>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        ready!(self.0.poll_unpin(cx))?;

        let response = Response::builder()
            .status(StatusCode::OK)
            .body(Empty::new())
            .map_err(map_http_error);

        Poll::Ready(response)
    }
}

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

pub enum ExportSink {
    RecvChan(
        SendFut<'static, ExportDataReceiver>,
        Option<ExportDataSender>,
    ),
    SendData(SendSink<'static, Bytes>),
}

impl Sink<Bytes> for ExportSink {
    type Error = io::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        loop {
            let state = match self.as_mut().get_mut() {
                ExportSink::RecvChan(send_fut, sender) => {
                    ready!(send_fut.poll_unpin(cx)).map_err(channel_pipe_error)?;
                    ExportSink::SendData(sender.take().unwrap().into_sink())
                }
                ExportSink::SendData(sink) => {
                    return sink.poll_ready_unpin(cx).map_err(data_pipe_error)
                }
            };

            self.set(state);
        }
    }

    fn start_send(self: Pin<&mut Self>, item: Bytes) -> Result<(), Self::Error> {
        match self.get_mut() {
            ExportSink::RecvChan(..) => {
                unreachable!("start_send is always preceded by poll_ready")
            }
            ExportSink::SendData(sink) => sink.start_send_unpin(item).map_err(data_pipe_error),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        loop {
            let state = match self.as_mut().get_mut() {
                ExportSink::RecvChan(send_fut, sender) => {
                    ready!(send_fut.poll_unpin(cx)).map_err(channel_pipe_error)?;
                    ExportSink::SendData(sender.take().unwrap().into_sink())
                }
                ExportSink::SendData(sink) => {
                    return sink.poll_flush_unpin(cx).map_err(data_pipe_error)
                }
            };

            self.set(state);
        }
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        loop {
            let state = match self.as_mut().get_mut() {
                ExportSink::RecvChan(send_fut, sender) => {
                    ready!(send_fut.poll_unpin(cx)).map_err(channel_pipe_error)?;
                    ExportSink::SendData(sender.take().unwrap().into_sink())
                }
                ExportSink::SendData(sink) => {
                    return sink.poll_close_unpin(cx).map_err(data_pipe_error)
                }
            };

            self.set(state);
        }
    }
}
