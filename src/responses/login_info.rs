use serde::Deserialize;

use crate::options::ProtocolVersion;

/// Struct representing database information returned
/// after establishing a connection.
#[allow(unused)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoginInfo {
    pub protocol_version: ProtocolVersion,
    pub session_id: u64,
    pub release_version: String,
    pub database_name: String,
    pub product_name: String,
    pub max_data_message_size: u64,
    pub max_identifier_length: u64,
    pub max_varchar_length: u64,
    pub identifier_quote_string: String,
    pub time_zone: String,
    pub time_zone_behavior: String,
}
