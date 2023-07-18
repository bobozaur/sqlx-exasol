use std::{future, sync::Arc};

use either::Either;
use sqlx::{Connection, Database, Error as SqlxError, Executor};

use async_tungstenite::{
    tungstenite::{protocol::Role, Message},
    WebSocketStream,
};

use futures_io::{AsyncRead, AsyncWrite};
use futures_util::{stream, SinkExt, StreamExt, TryStreamExt};

use crate::{
    arguments::ExaArguments,
    command::{CloseResultSet, Command, ExecutePreparedStmt, Fetch, SqlText},
    con_opts::ExaConnectOptions,
    database::Exasol,
    responses::{
        fetched::DataChunk,
        result::{ExaResultStream, ExecutionResults, ExecutionResultsStream},
        Response, ResponseData,
    },
    statement::ExaStatementMetadata,
};

pub trait Socket: AsyncRead + AsyncWrite + std::fmt::Debug + Send + Unpin {}

#[derive(Debug)]
pub struct ExaConnection {
    ws: WebSocketStream<Box<dyn Socket>>,
    use_compression: bool,
    pub(crate) last_result_set_handles: Option<Vec<u16>>,
}

impl ExaConnection {
    pub async fn new(stream: Box<dyn Socket>) -> Self {
        Self {
            ws: WebSocketStream::from_raw_socket(stream, Role::Client, None).await,
            use_compression: false,
            last_result_set_handles: None,
        }
    }

    pub(crate) async fn close_previous_result_sets(&mut self) -> Result<(), String> {
        if let Some(handles) = self.last_result_set_handles.take() {
            let cmd = Command::CloseResultSet(CloseResultSet::new(handles));
            self.send_cmd(&cmd).await?;
        };
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<(), String> {
        self.send_cmd(&Command::Disconnect).await?;
        Ok(())
    }

    async fn close_ws(&mut self) -> Result<(), String> {
        self.ws.close(None).await.map_err(|e| e.to_string())?;
        Ok(())
    }

    pub(crate) async fn fetch_chunk(&mut self, fetch_cmd: Fetch) -> Result<DataChunk, String> {
        let resp_data = self.get_resp_data(&Command::Fetch(fetch_cmd)).await?;

        match resp_data {
            ResponseData::FetchedData(f) => Ok(f),
            _ => Err("Expected fetched data response".to_owned()),
        }
    }

    async fn get_or_prepare(&mut self) -> ExecutePreparedStmt {
        todo!()
    }

    pub(crate) async fn execute_query(
        &mut self,
        sql: String,
        metadata: Option<Arc<ExaStatementMetadata>>,
        arguments: Option<ExaArguments>,
        persistent: bool,
    ) -> Result<ExecutionResultsStream<'_>, String> {
        let command = if let Some(arguments) = arguments {
            Command::ExecutePreparedStatement(self.get_or_prepare().await)
        } else {
            Command::Execute(SqlText::new(sql))
        };

        self.get_results_stream(command).await
    }

    pub(crate) async fn get_results_stream(
        &mut self,
        command: Command,
    ) -> Result<ExecutionResultsStream<'_>, String> {
        self.close_previous_result_sets().await?;

        let resp_data = self.get_resp_data(&command).await?;

        match resp_data {
            ResponseData::Results(r) => ExecutionResultsStream::new(self, r),
            _ => Err("Expected results response".to_owned()),
        }
    }

    async fn get_resp_data(&mut self, command: &Command) -> Result<ResponseData, String> {
        self.send_cmd(command)
            .await?
            .ok_or_else(|| "No response data received".to_owned())
    }

    async fn send_cmd(&mut self, command: &Command) -> Result<Option<ResponseData>, String> {
        let msg_string = serde_json::to_string(command).unwrap();

        // if self.use_compression {
        //     // #[cfg(feature = "flate2")]
        //     self.send_compressed_cmd(msg_string)
        // } else {
        //     self.send_uncompressed_cmd(msg_string)
        // }

        let response = self.send_uncompressed_cmd(msg_string).await?;

        match response {
            Response::Ok {
                response_data,
                attributes,
            } => todo!(),
            Response::Error { exception } => Err(exception.to_string()),
        }
    }

    async fn send_uncompressed_cmd(&mut self, msg_string: String) -> Result<Response, String> {
        self.ws
            .send(Message::Text(msg_string))
            .await
            .map_err(|e| e.to_string())?;

        while let Some(response) = self.ws.next().await {
            let msg = response.map_err(|e| e.to_string())?;

            return match msg {
                Message::Text(s) => serde_json::from_str(&s).map_err(|e| e.to_string())?,
                Message::Binary(v) => serde_json::from_slice(&v).map_err(|e| e.to_string())?,
                Message::Close(c) => Err("Close frame received".to_owned()),
                _ => continue,
            };
        }

        Err("No message received".to_owned())
    }

    async fn ping(&mut self) -> Result<(), String> {
        self.ws
            .send(Message::Ping(Vec::new()))
            .await
            .map_err(|e| e.to_string())?;

        Ok(())
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

    type Options = ExaConnectOptions;

    fn close(mut self) -> futures_util::future::BoxFuture<'static, Result<(), sqlx::Error>> {
        Box::pin(async move {
            self.disconnect().await.map_err(SqlxError::Protocol)?;

            self.close_ws()
                .await
                .map_err(|e| SqlxError::Protocol(e.to_string()))?;

            Ok(())
        })
    }

    fn close_hard(mut self) -> futures_util::future::BoxFuture<'static, Result<(), sqlx::Error>> {
        Box::pin(async move {
            self.close_ws()
                .await
                .map_err(|e| SqlxError::Protocol(e.to_string()))?;

            Ok(())
        })
    }

    fn ping(&mut self) -> futures_util::future::BoxFuture<'_, Result<(), sqlx::Error>> {
        Box::pin(async move {
            self.ping().await.map_err(SqlxError::Protocol)?;
            Ok(())
        })
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

    fn shrink_buffers(&mut self) {}

    fn flush(&mut self) -> futures_util::future::BoxFuture<'_, Result<(), sqlx::Error>> {
        Box::pin(future::ready(Ok(())))
    }

    fn should_flush(&self) -> bool {
        false
    }
}

impl<'c> Executor<'c> for &'c mut ExaConnection {
    type Database = Exasol;

    fn fetch_many<'e, 'q: 'e, E: 'q>(
        self,
        mut query: E,
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
        let sql = query.sql().to_owned();
        let metadata = query.statement().map(|s| s.metadata.clone());
        let arguments = query.take_arguments();
        let persistent = query.persistent();
        let future = self.execute_query(sql, metadata, arguments, persistent);
        Box::pin(ExaResultStream::new(future).map_err(SqlxError::Protocol))
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
        let mut s = self.fetch_many(query);

        Box::pin(async move {
            while let Some(v) = s.try_next().await? {
                if let Either::Right(r) = v {
                    return Ok(Some(r));
                }
            }

            Ok(None)
        })
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
