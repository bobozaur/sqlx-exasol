mod error;
pub(crate) mod fetched;
mod hosts;
mod login_info;
pub(crate) mod prepared_stmt;
mod pub_key;
pub(crate) mod result;

use serde::{de::Error, Deserialize, Deserializer, Serialize};

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
#[allow(dead_code)]
#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Attributes {
    // Read-write attributes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) autocommit: Option<bool>,
    pub(crate) current_schema: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) feedback_interval: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) numeric_characters: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) query_timeout: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) snapshot_transactions_enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) timestamp_utc_enabled: Option<bool>,
    // Read-only attributes
    #[serde(skip_serializing)]
    pub(crate) compression_enabled: Option<bool>,
    #[serde(skip_serializing)]
    pub(crate) date_format: Option<String>,
    #[serde(skip_serializing)]
    pub(crate) date_language: Option<String>,
    #[serde(skip_serializing)]
    pub(crate) datetime_format: Option<String>,
    #[serde(skip_serializing)]
    pub(crate) default_like_escape_character: Option<String>,
    #[serde(skip_serializing)]
    #[serde(default)]
    #[serde(deserialize_with = "Attributes::deserialize_open_transaction")]
    pub(crate) open_transaction: Option<bool>,
    #[serde(skip_serializing)]
    pub(crate) timezone: Option<String>,
    #[serde(skip_serializing)]
    pub(crate) timezone_behavior: Option<String>,
}

impl Attributes {
    pub fn update(&mut self, other: Self) {
        macro_rules! other_or_prev {
            ($field:tt) => {
                if let Some(new) = other.$field {
                    self.$field = Some(new);
                }
            };
        }

        other_or_prev!(autocommit);
        other_or_prev!(current_schema);
        other_or_prev!(feedback_interval);
        other_or_prev!(numeric_characters);
        other_or_prev!(query_timeout);
        other_or_prev!(snapshot_transactions_enabled);
        other_or_prev!(timestamp_utc_enabled);
        other_or_prev!(compression_enabled);
        other_or_prev!(date_format);
        other_or_prev!(date_language);
        other_or_prev!(datetime_format);
        other_or_prev!(default_like_escape_character);
        other_or_prev!(open_transaction);
        other_or_prev!(timezone);
        other_or_prev!(timezone_behavior);
    }

    fn deserialize_open_transaction<'de, D>(deserializer: D) -> Result<Option<bool>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let Some(value) = Option::deserialize(deserializer)? else {return Ok(None)};

        match value {
            0 => Ok(Some(false)),
            1 => Ok(Some(true)),
            v => Err(D::Error::custom(format!(
                "Invalid value for 'open_transaction' field: {v}"
            ))),
        }
    }
}
