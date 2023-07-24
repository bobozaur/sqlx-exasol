mod error;
pub(crate) mod fetched;
mod hosts;
mod login_info;
pub(crate) mod prepared_stmt;
mod pub_key;
pub(crate) mod result;

use serde::{Deserialize, Serialize};

use self::{
    fetched::DataChunk, hosts::Hosts, login_info::LoginInfo, prepared_stmt::PreparedStatement,
    pub_key::PublicKey, result::StmtResult,
};

pub use error::DatabaseError;

/// Generic response received from the Exasol server
/// This is the first deserialization step
/// Used to determine whether the message
/// is a proper response, or an error
///
/// We're forced to use internal tagging as
/// ok/error responses have different adjacent fields
#[derive(Debug, Deserialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum Response {
    #[serde(rename_all = "camelCase")]
    Ok {
        response_data: Option<ResponseData>,
        attributes: Option<Attributes>,
    },
    Error {
        exception: DatabaseError,
    },
}

/// This is the `responseData` field of the JSON response.
/// Because all `ok` responses are returned through this
/// with no specific identifier between them
/// we have to use untagged deserialization.
///
/// As a result, the order of the enum variants matters,
/// as deserialization has to be non-overlapping yet exhaustive.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum ResponseData {
    PreparedStatement(PreparedStatement),
    Results(StmtResult),
    FetchedData(DataChunk),
    Hosts(Hosts),
    LoginInfo(LoginInfo),
    PublicKey(PublicKey),
}

/// Struct representing attributes returned from Exasol.
/// These can either be returned by an explicit `getAttributes` call
/// or as part of any response.
///
/// Note that some of these are *read-only*!
/// See the [specification](<https://github.com/exasol/websocket-api/blob/master/docs/WebsocketAPIV1.md#attributes-session-and-database-properties>)
/// for more details.
#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Attributes {
    pub(crate) autocommit: bool,
    pub(crate) compression_enabled: bool,
    pub(crate) current_schema: String,
    pub(crate) date_format: String,
    pub(crate) date_language: String,
    pub(crate) datetime_format: String,
    pub(crate) default_like_escape_character: String,
    pub(crate) feedback_interval: u32,
    pub(crate) numeric_characters: String,
    pub(crate) open_transaction: bool,
    pub(crate) query_timeout: u64,
    pub(crate) snapshot_transactions_enabled: bool,
    pub(crate) timestamp_utc_enabled: bool,
    pub(crate) timezone: String,
    pub(crate) timezone_behavior: String,
}
