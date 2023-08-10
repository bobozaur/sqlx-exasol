use std::net::IpAddr;

use serde::{ser::Error, Serialize};
use serde_json::value::RawValue;
use sqlx_core::Error as SqlxError;

use crate::{
    arguments::{ExaBuffer, NumParamSets},
    column::ExaColumn,
    options::{ExaConnectOptionsRef, ProtocolVersion},
    responses::ExaAttributes,
};

/// Enum encapsulating database requests, differentiated by the `command` tag.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
#[serde(tag = "command")]
pub(crate) enum ExaCommand<'a> {
    Disconnect,
    GetAttributes,
    SetAttributes(SetAttributes<'a>),
    Login(LoginInfo),
    LoginToken(LoginInfo),
    GetHosts(GetHosts),
    Execute(Sql<'a>),
    Fetch(Fetch),
    CloseResultSet(CloseResultSet),
    CreatePreparedStatement(Sql<'a>),
    ExecutePreparedStatement(ExecutePreparedStmt<'a>),
    ClosePreparedStatement(ClosePreparedStmt),
    #[cfg(feature = "migrate")]
    ExecuteBatch(BatchSql<'a>),
}

impl<'a> ExaCommand<'a> {
    pub fn new_set_attributes(attributes: &'a ExaAttributes) -> Self {
        Self::SetAttributes(SetAttributes { attributes })
    }

    pub fn new_login(protocol_version: ProtocolVersion) -> Self {
        Self::Login(LoginInfo { protocol_version })
    }

    pub fn new_login_token(protocol_version: ProtocolVersion) -> Self {
        Self::LoginToken(LoginInfo { protocol_version })
    }

    pub fn new_get_hosts(host_ip: IpAddr) -> Self {
        Self::GetHosts(GetHosts { host_ip })
    }

    pub fn new_execute(sql: &'a str, attributes: &'a ExaAttributes) -> Self {
        Self::Execute(Sql {
            attributes: Some(attributes),
            sql_text: sql,
        })
    }

    pub fn new_fetch(handle: u16, pos: usize, num_bytes: usize) -> Self {
        Self::Fetch(Fetch {
            result_set_handle: handle,
            start_position: pos,
            num_bytes,
        })
    }

    pub fn new_close_result(handle: u16) -> Self {
        Self::CloseResultSet(CloseResultSet {
            result_set_handles: [handle],
        })
    }

    pub fn new_create_prepared(sql: &'a str) -> Self {
        Self::CreatePreparedStatement(Sql {
            attributes: None,
            sql_text: sql,
        })
    }

    pub fn new_execute_prepared(
        handle: u16,
        columns: &'a [ExaColumn],
        buf: ExaBuffer,
        attributes: &'a ExaAttributes,
    ) -> Result<Self, SqlxError> {
        let prepared = ExecutePreparedStmt::new(handle, columns, buf, attributes)?;
        Ok(Self::ExecutePreparedStatement(prepared))
    }

    pub fn new_close_prepared(handle: u16) -> Self {
        Self::ClosePreparedStatement(ClosePreparedStmt {
            statement_handle: handle,
        })
    }

    #[cfg(feature = "migrate")]
    pub fn new_execute_batch(sql_batch: Vec<&'a str>, attributes: &'a ExaAttributes) -> Self {
        Self::ExecuteBatch(BatchSql {
            attributes,
            sql_texts: sql_batch,
        })
    }
}

/// Represents a serialized command, ready to be sent to the server.
// We use this wrapper so that we can serialize the command (which would happen anyway)
// and get rid of lifetimes and borrow checker conflicts sooner.
//
// The wrapper ensures that the only ways of creating a command
// is through the implemented conversions.
#[derive(Debug)]
pub struct Command(String);

impl Command {
    pub(crate) fn into_inner(self) -> String {
        self.0
    }
}

impl<'a> TryFrom<ExaCommand<'a>> for Command {
    type Error = SqlxError;

    fn try_from(value: ExaCommand<'a>) -> Result<Self, Self::Error> {
        serde_json::to_string(&value)
            .map_err(|e| SqlxError::Protocol(e.to_string()))
            .map(Self)
    }
}

impl<'a> TryFrom<&'a ExaConnectOptionsRef<'a>> for Command {
    type Error = SqlxError;

    fn try_from(value: &'a ExaConnectOptionsRef<'a>) -> Result<Self, Self::Error> {
        serde_json::to_string(value)
            .map_err(|e| SqlxError::Protocol(e.to_string()))
            .map(Self)
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SetAttributes<'a> {
    attributes: &'a ExaAttributes,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LoginInfo {
    protocol_version: ProtocolVersion,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GetHosts {
    host_ip: IpAddr,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Sql<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    attributes: Option<&'a ExaAttributes>,
    sql_text: &'a str,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BatchSql<'a> {
    attributes: &'a ExaAttributes,
    sql_texts: Vec<&'a str>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SqlBatch {
    sql_texts: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Fetch {
    result_set_handle: u16,
    start_position: usize,
    num_bytes: usize,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CloseResultSet {
    result_set_handles: [u16; 1],
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExecutePreparedStmt<'a> {
    attributes: &'a ExaAttributes,
    statement_handle: u16,
    num_columns: usize,
    num_rows: usize,
    #[serde(skip_serializing_if = "<[ExaColumn]>::is_empty")]
    columns: &'a [ExaColumn],
    #[serde(skip_serializing_if = "PreparedStmtData::is_empty")]
    data: PreparedStmtData,
}

impl<'a> ExecutePreparedStmt<'a> {
    fn new(
        handle: u16,
        columns: &'a [ExaColumn],
        data: ExaBuffer,
        attributes: &'a ExaAttributes,
    ) -> Result<Self, SqlxError> {
        let prepared = Self {
            attributes,
            statement_handle: handle,
            num_columns: columns.len(),
            num_rows: data.num_param_sets()?,
            columns,
            data: data.into(),
        };

        Ok(prepared)
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ClosePreparedStmt {
    statement_handle: u16,
}

/// Type containing the parameters data to be passed
/// as part of executing a prepared statement.
///
/// The type ensures the parameter sequence in the
/// [`ExaBuffer`] is appropriately ended.
#[derive(Debug, Clone)]
struct PreparedStmtData {
    inner: Vec<u8>,
    num_rows: NumParamSets,
}

impl PreparedStmtData {
    fn is_empty(&self) -> bool {
        matches!(self.num_rows, NumParamSets::NotSet | NumParamSets::Set(0))
    }
}

impl Serialize for PreparedStmtData {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let raw_value: &RawValue = serde_json::from_slice(&self.inner).map_err(Error::custom)?;
        raw_value.serialize(serializer)
    }
}

impl From<ExaBuffer> for PreparedStmtData {
    fn from(mut value: ExaBuffer) -> Self {
        value.finalize();

        Self {
            inner: value.inner,
            num_rows: value.num_param_sets,
        }
    }
}
