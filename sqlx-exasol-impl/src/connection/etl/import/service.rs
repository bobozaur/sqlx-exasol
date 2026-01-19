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
    error::{map_http_error, map_hyper_err, send_channel_error},
    import::{ImportChannelSender, ImportDataReceiver, ImportDataSender},
};

/// A [`hyper`] service that handles an `IMPORT` request from Exasol.
///
/// When an `IMPORT` job is started, Exasol makes an HTTP `GET` request to the
/// one-shot server, expecting to receive the data to be imported in the response body.
/// This service streams data from the [`crate::connection::etl::ExaImport`] worker to the response
/// body.
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

    /// Once a request is received, create the data channel between the HTTP server
    /// and the [`crate::connection::etl::ExaImport`] and send the sender end.
    ///
    /// We can then use the receiver to pipe data through.
    fn call(&self, req: Request<Incoming>) -> Self::Future {
        let (tx, rx) = flume::bounded(0);
        let send_fut = self.0.clone().into_send_async(tx);
        ImportFuture::new(req, ImportStream::SendChan(send_fut, Some(rx)))
    }
}

/// The future returned by [`ImportService`].
///
/// This future immediately returns a `200 OK` response with the [`ImportStream`] as the body.
/// The [`ImportStream`] will then be polled by [`hyper`] to send the data to Exasol.
/// It also consumes the request body from Exasol, which is expected to be empty for `IMPORT`.
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
        // We don't expect anything other than an empty body but
        // consume request body frames before sending response just in case.
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

/// A [`hyper`] [`Body`] that streams data from an [`crate::connection::etl::ExaImport`] worker.
///
/// This enum represents the two states of the stream:
/// 1. [`ImportStream::SendChan`]: The stream has not yet sent the data channel to the worker. The
///    channel will be sent after the HTTP request is received and processing starts.
/// 2. [`ImportStream::RecvData`]: The channel has been sent, and the stream is now receiving data
///    from the worker and forwarding it to [`hyper`].
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
                    ready!(send_fut.poll_unpin(cx)).map_err(send_channel_error)?;
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
