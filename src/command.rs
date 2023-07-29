use std::net::IpAddr;

use serde::Serialize;
use serde_json::Value;

use crate::{column::ExaColumn, options::ProtocolVersion, responses::Attributes};

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
#[serde(tag = "command")]
pub enum Command<'a> {
    Disconnect,
    GetAttributes,
    SetAttributes(SetAttributes<'a>),
    Login(LoginInfo),
    LoginToken(LoginInfo),
    GetHosts(GetHosts),
    Execute(SqlText<'a>),
    Fetch(Fetch),
    CloseResultSet(CloseResultSet),
    CreatePreparedStatement(SqlText<'a>),
    ExecutePreparedStatement(ExecutePreparedStmt<'a>),
    ClosePreparedStatement(ClosePreparedStmt),
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SetAttributes<'a> {
    attributes: &'a Attributes,
}

impl<'a> SetAttributes<'a> {
    pub fn new(attributes: &'a Attributes) -> Self {
        Self { attributes }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LoginInfo {
    protocol_version: ProtocolVersion,
}

impl LoginInfo {
    pub fn new(protocol_version: ProtocolVersion) -> Self {
        Self { protocol_version }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetHosts {
    host_ip: IpAddr,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SqlText<'a> {
    sql_text: &'a str,
}

impl<'a> SqlText<'a> {
    pub fn new(sql: &'a str) -> Self {
        Self { sql_text: sql }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SqlBatch {
    sql_texts: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
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

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CloseResultSet {
    result_set_handles: [u16; 1],
}

impl CloseResultSet {
    pub fn new(handle: u16) -> Self {
        Self {
            result_set_handles: [handle],
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutePreparedStmt<'a> {
    statement_handle: u16,
    num_columns: u8,
    num_rows: u8,
    #[serde(skip_serializing_if = "Self::has_no_columns")]
    columns: &'a [ExaColumn],
    #[serde(skip_serializing_if = "Vec::is_empty")]
    data: Vec<[Value; 1]>,
}

impl<'a> ExecutePreparedStmt<'a> {
    pub fn new(handle: u16, columns: &'a [ExaColumn], data: Vec<[Value; 1]>) -> Self {
        Self {
            statement_handle: handle,
            num_columns: columns.len() as u8,
            num_rows: (!data.is_empty()).into(),
            columns,
            data,
        }
    }

    fn has_no_columns(columns: &[ExaColumn]) -> bool {
        columns.is_empty()
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClosePreparedStmt {
    statement_handle: u16,
}

impl ClosePreparedStmt {
    pub fn new(handle: u16) -> Self {
        Self {
            statement_handle: handle,
        }
    }
}
