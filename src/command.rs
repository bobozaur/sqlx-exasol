use std::net::IpAddr;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[serde(tag = "command")]
pub enum Command {
    Disconnect,
    GetAttributes,
    SetAttributes(SetAttributes),
    Login(LoginInfo),
    LoginToken(LoginInfo),
    GetHosts(GetHosts),
    Execute(SqlText),
    ExecuteBatch(SqlBatch),
    Fetch(Fetch),
    CloseResultSet(CloseResultSet),
    CreatePreparedStatement(SqlText),
    ExecutePreparedStatement(ExecutePreparedStmt),
    ClosePreparedStatement(ClosePreparedStmt),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SetAttributes {
    attributes: Option<()>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LoginInfo {
    protocol_version: Option<()>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetHosts {
    host_ip: IpAddr,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SqlText {
    sql_text: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SqlBatch {
    sql_texts: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Fetch {
    result_set_handle: u16,
    start_position: usize,
    num_bytes: usize,
}

impl Fetch {
    pub fn new(result_set_handle: u16, start_position: usize, num_bytes: usize) -> Self {
        Self {
            result_set_handle,
            start_position,
            num_bytes,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CloseResultSet {
    result_set_handles: [u16; 1],
}

impl From<u16> for CloseResultSet {
    fn from(value: u16) -> Self {
        Self {
            result_set_handles: [value],
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutePreparedStmt {
    statement_handle: u16,
    num_columns: u8,
    num_rows: u64,
    columns: Option<()>,
    data: Option<()>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClosePreparedStmt {
    statement_handle: u16,
}
