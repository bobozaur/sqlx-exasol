use sqlx::{Connection, Database, Executor};

use async_tungstenite::{
    tungstenite::{protocol::Role, Message},
    WebSocketStream,
};

use futures_io::{AsyncRead, AsyncWrite};
use futures_util::{SinkExt, StreamExt};

use crate::{command::Command, con_opts::ConOpts, database::Exasol};

pub trait Socket: AsyncRead + AsyncWrite + std::fmt::Debug + Send + Unpin {}

#[derive(Debug)]
pub struct ExaConnection {
    ws: WebSocketStream<Box<dyn Socket>>,
    use_compression: bool,
}

impl ExaConnection {
    pub async fn new(stream: Box<dyn Socket>) -> Self {
        Self {
            ws: WebSocketStream::from_raw_socket(stream, Role::Client, None).await,
            use_compression: false,
        }
    }

    pub async fn execute(&self, query: &str) {
        // self.ws.send(Message)
    }

    async fn send_cmd(&mut self, command: &Command) {
        let msg_string = serde_json::to_string(command).unwrap();

        // if self.use_compression {
        //     // #[cfg(feature = "flate2")]
        //     self.send_compressed_cmd(msg_string)
        // } else {
        //     self.send_uncompressed_cmd(msg_string)
        // }

        // self.send_uncompressed_cmd(msg_string)
    }

    async fn send_uncompressed_cmd(&mut self, msg_string: String) {
        self.ws.send(Message::Text(msg_string)).await.unwrap();

        while let Some(response) = self.ws.next().await {
            return match response.unwrap() {
                Message::Text(s) => (),
                Message::Binary(v) => (),
                Message::Close(c) => (),
                _ => continue,
            };
        }
    }

    // #[cfg(feature = "flate2")]
    // async fn send_compressed_cmd(&mut self, msg_string: String) {
    //     let msg = msg_string.as_bytes();
    //     let mut buf = Vec::new();
    //     ZlibEncoder::new(msg, Compression::default()).read_to_end(&mut buf);
    //     self.ws.send(Message::Binary(buf)).await.unwrap();

    //     while let Some(response) = self.ws.next().await {
    //         return match response.unwrap() {
    //             Message::Text(s) => serde_json::from_reader(ZlibDecoder::new(s.as_bytes())),
    //             Message::Binary(v) => serde_json::from_reader(ZlibDecoder::new(v.as_slice())),
    //             Message::Close(c) => (),
    //             _ => continue,
    //         };
    //     }
    // }
}

impl Connection for ExaConnection {
    type Database = Exasol;

    type Options = ConOpts;

    fn close(self) -> futures_util::future::BoxFuture<'static, Result<(), sqlx::Error>> {
        todo!()
    }

    fn close_hard(self) -> futures_util::future::BoxFuture<'static, Result<(), sqlx::Error>> {
        todo!()
    }

    fn ping(&mut self) -> futures_util::future::BoxFuture<'_, Result<(), sqlx::Error>> {
        todo!()
    }

    fn begin(
        &mut self,
    ) -> futures_util::future::BoxFuture<
        '_,
        Result<sqlx::Transaction<'_, Self::Database>, sqlx::Error>,
    >
    where
        Self: Sized,
    {
        todo!()
    }

    fn shrink_buffers(&mut self) {
        todo!()
    }

    fn flush(&mut self) -> futures_util::future::BoxFuture<'_, Result<(), sqlx::Error>> {
        todo!()
    }

    fn should_flush(&self) -> bool {
        todo!()
    }
}

impl<'c> Executor<'c> for &'c mut ExaConnection {
    type Database = Exasol;

    fn fetch_many<'e, 'q: 'e, E: 'q>(
        self,
        query: E,
    ) -> futures_util::stream::BoxStream<
        'e,
        Result<
            sqlx::Either<
                <Self::Database as Database>::QueryResult,
                <Self::Database as Database>::Row,
            >,
            sqlx::Error,
        >,
    >
    where
        'c: 'e,
        E: sqlx::Execute<'q, Self::Database>,
    {
        todo!()
    }

    fn fetch_optional<'e, 'q: 'e, E: 'q>(
        self,
        query: E,
    ) -> futures_util::future::BoxFuture<
        'e,
        Result<Option<<Self::Database as Database>::Row>, sqlx::Error>,
    >
    where
        'c: 'e,
        E: sqlx::Execute<'q, Self::Database>,
    {
        todo!()
    }

    fn prepare_with<'e, 'q: 'e>(
        self,
        sql: &'q str,
        parameters: &'e [<Self::Database as Database>::TypeInfo],
    ) -> futures_util::future::BoxFuture<
        'e,
        Result<<Self::Database as sqlx::database::HasStatement<'q>>::Statement, sqlx::Error>,
    >
    where
        'c: 'e,
    {
        todo!()
    }

    fn describe<'e, 'q: 'e>(
        self,
        sql: &'q str,
    ) -> futures_util::future::BoxFuture<'e, Result<sqlx::Describe<Self::Database>, sqlx::Error>>
    where
        'c: 'e,
    {
        todo!()
    }
}
