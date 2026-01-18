use std::{
    future::Future,
    io,
    pin::Pin,
    task::{ready, Context, Poll},
};

use flume::r#async::{RecvStream, SendFut};
use futures_util::{FutureExt, Stream, StreamExt};
use hyper::{
    body::{Body, Bytes, Frame, Incoming},
    service::Service,
    Request, Response, StatusCode,
};
use sqlx_core::bytes::BytesMut;

use crate::etl::{
    error::{channel_pipe_error, map_http_error, map_hyper_err},
    import::{ImportChannelSender, ImportDataReceiver, ImportDataSender},
};

pub struct ImportService(ImportChannelSender);

impl ImportService {
    pub fn new(tx: ImportChannelSender) -> Self {
        Self(tx)
    }
}

impl Service<Request<Incoming>> for ImportService {
    type Response = Response<ImportStream>;

    type Error = io::Error;

    type Future = ImportFuture;

    fn call(&self, req: Request<Incoming>) -> Self::Future {
        let (tx, rx) = flume::bounded(0);
        let send_fut = self.0.clone().into_send_async(tx);
        ImportFuture::new(req, ImportStream::SendChan(send_fut, Some(rx)))
    }
}

pub struct ImportFuture {
    inner: Incoming,
    stream: Option<ImportStream>,
}

impl ImportFuture {
    pub fn new(req: Request<Incoming>, rx: ImportStream) -> Self {
        Self {
            inner: req.into_body(),
            stream: Some(rx),
        }
    }
}

impl Future for ImportFuture {
    type Output = io::Result<Response<ImportStream>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Consume request body frames before sending response
        while ready!(Pin::new(&mut self.inner).poll_frame(cx))
            .transpose()
            .map_err(map_hyper_err)?
            .is_some()
        {}

        let response = Response::builder()
            .status(StatusCode::OK)
            .body(self.stream.take().unwrap())
            .map_err(map_http_error);

        Poll::Ready(response)
    }
}

pub enum ImportStream {
    SendChan(
        SendFut<'static, ImportDataSender>,
        Option<ImportDataReceiver>,
    ),
    RecvData(RecvStream<'static, BytesMut>),
}

impl Stream for ImportStream {
    type Item = Result<Frame<Bytes>, io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let state = match self.as_mut().get_mut() {
                ImportStream::SendChan(send_fut, receiver) => {
                    ready!(send_fut.poll_unpin(cx)).map_err(channel_pipe_error)?;
                    ImportStream::RecvData(receiver.take().unwrap().into_stream())
                }
                ImportStream::RecvData(receiver) => {
                    let out = ready!(receiver.poll_next_unpin(cx))
                        .map(From::from)
                        .map(Frame::data)
                        .map(Ok);

                    return Poll::Ready(out);
                }
            };

            self.set(state);
        }
    }
}

impl Body for ImportStream {
    type Data = Bytes;

    type Error = io::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        self.poll_next_unpin(cx)
    }
}
