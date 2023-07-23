use std::borrow::Cow;

use async_tungstenite::{tungstenite::Message, WebSocketStream};
use futures_io::{AsyncRead, AsyncWrite};
use futures_util::{Future, SinkExt, StreamExt};
use lru::LruCache;

use crate::{
    command::{ClosePreparedStmt, CloseResultSet, Command, Fetch, SqlText},
    responses::{
        fetched::DataChunk, prepared_stmt::PreparedStatement, result::QueryResult, Response,
        ResponseData,
    },
    stream::QueryResultStream,
};

pub trait Socket: AsyncRead + AsyncWrite + std::fmt::Debug + Send + Unpin {}

#[derive(Debug)]
pub struct ExaWebSocket(pub WebSocketStream<Box<dyn Socket>>);

impl ExaWebSocket {
    pub async fn get_results_stream<'a, C, F>(
        &'a mut self,
        command: Command<'_>,
        rs_handle: &mut Option<u16>,
        fetch_maker: C,
    ) -> Result<QueryResultStream<'_, C, F>, String>
    where
        C: FnMut(&'a mut ExaWebSocket, Fetch) -> F,
        F: Future<Output = Result<(DataChunk, &'a mut ExaWebSocket), String>> + 'a,
    {
        if let Some(handle) = rs_handle {
            self.close_result_set(*handle).await?;
        }

        let query_result = self.get_results(command).await?;
        std::mem::swap(rs_handle, &mut query_result.handle());

        QueryResultStream::new(self, query_result, fetch_maker)
    }

    pub async fn get_results(&mut self, command: Command<'_>) -> Result<QueryResult, String> {
        let resp_data = self.get_resp_data(command).await?;

        match resp_data {
            ResponseData::Results(r) => Ok(r.results.into_iter().next().unwrap()),
            _ => Err("Expected results response".to_owned()),
        }
    }

    pub async fn close_result_set(&mut self, handle: u16) -> Result<(), String> {
        let command = Command::CloseResultSet(CloseResultSet::new(handle));
        self.send_cmd(command).await?;
        Ok(())
    }

    pub async fn create_prepared(
        &mut self,
        command: Command<'_>,
    ) -> Result<PreparedStatement, String> {
        let resp_data = self.get_resp_data(command).await?;

        match resp_data {
            ResponseData::PreparedStatement(p) => Ok(p),
            _ => Err("Expected prepared statement response".to_owned()),
        }
    }

    pub async fn close_prepared(&mut self, handle: u16) -> Result<(), String> {
        let command = Command::ClosePreparedStatement(ClosePreparedStmt::new(handle));
        self.send_cmd(command).await?;
        Ok(())
    }

    pub async fn fetch_chunk(
        &mut self,
        fetch_cmd: Fetch,
    ) -> Result<(DataChunk, &mut Self), String> {
        let resp_data = self.get_resp_data(Command::Fetch(fetch_cmd)).await?;

        match resp_data {
            ResponseData::FetchedData(f) => Ok((f, self)),
            _ => Err("Expected fetched data response".to_owned()),
        }
    }

    pub async fn commit(&mut self) -> Result<(), String> {
        self.send_cmd(Command::Execute(SqlText::new("COMMIT;")))
            .await?;
        Ok(())
    }

    pub async fn rollback(&mut self) -> Result<(), String> {
        self.send_cmd(Command::Execute(SqlText::new("ROLLBACK;")))
            .await?;
        Ok(())
    }

    pub async fn ping(&mut self) -> Result<(), String> {
        self.0
            .send(Message::Ping(Vec::new()))
            .await
            .map_err(|e| e.to_string())?;

        Ok(())
    }

    pub async fn disconnect(&mut self) -> Result<(), String> {
        self.send_cmd(Command::Disconnect).await?;
        Ok(())
    }

    pub async fn close(&mut self) -> Result<(), String> {
        self.0.close(None).await.map_err(|e| e.to_string())?;
        Ok(())
    }

    pub async fn get_or_prepare<'a>(
        &mut self,
        cache: &'a mut LruCache<String, PreparedStatement>,
        sql: &str,
        persist: bool,
    ) -> Result<Cow<'a, PreparedStatement>, String> {
        // The double look-up is required to avoid a borrow checker limitation.
        //
        // See: https://github.com/rust-lang/rust/issues/54663
        if cache.contains(sql) {
            return Ok(Cow::Borrowed(cache.get(sql).unwrap()));
        }

        let command = SqlText::new(sql);
        let prepared = self
            .create_prepared(Command::CreatePreparedStatement(command))
            .await?;

        if persist {
            if let Some(old) = cache.put(sql.to_owned(), prepared) {
                self.close_prepared(old.statement_handle).await?;
            }

            return Ok(Cow::Borrowed(cache.get(sql).unwrap()));
        }

        Ok(Cow::Owned(prepared))
    }

    async fn get_resp_data(&mut self, command: Command<'_>) -> Result<ResponseData, String> {
        self.send_cmd(command)
            .await?
            .ok_or_else(|| "No response data received".to_owned())
    }

    async fn send_cmd(&mut self, command: Command<'_>) -> Result<Option<ResponseData>, String> {
        let msg_string = serde_json::to_string(&command).unwrap();

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
        self.0
            .send(Message::Text(msg_string))
            .await
            .map_err(|e| e.to_string())?;

        while let Some(response) = self.0.next().await {
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
