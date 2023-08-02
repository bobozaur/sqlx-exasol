use serde::Deserialize;

use crate::options::ProtocolVersion;

/// Struct representing database information returned after establishing a connection.
#[allow(unused)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionInfo {
    pub(crate) protocol_version: ProtocolVersion,
    pub(crate) session_id: u64,
    pub(crate) release_version: String,
    pub(crate) database_name: String,
    pub(crate) product_name: String,
    pub(crate) max_data_message_size: u64,
    pub(crate) max_identifier_length: u64,
    pub(crate) max_varchar_length: u64,
    pub(crate) identifier_quote_string: String,
    pub(crate) time_zone: String,
    pub(crate) time_zone_behavior: String,
}
