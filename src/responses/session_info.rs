use serde::Deserialize;

use crate::options::ProtocolVersion;

/// Struct representing database information returned after establishing a connection.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionInfo {
    protocol_version: ProtocolVersion,
    session_id: u64,
    release_version: String,
    database_name: String,
    product_name: String,
    max_data_message_size: u64,
    max_identifier_length: u64,
    max_varchar_length: u64,
    identifier_quote_string: String,
    time_zone: String,
    time_zone_behavior: String,
}

impl SessionInfo {
    pub fn protocol_version(&self) -> ProtocolVersion {
        self.protocol_version
    }

    pub fn session_id(&self) -> u64 {
        self.session_id
    }

    pub fn release_version(&self) -> &str {
        &self.release_version
    }

    pub fn database_name(&self) -> &str {
        &self.database_name
    }

    pub fn product_name(&self) -> &str {
        &self.product_name
    }

    pub fn max_data_message_size(&self) -> u64 {
        self.max_data_message_size
    }

    pub fn max_identifier_length(&self) -> u64 {
        self.max_identifier_length
    }

    pub fn max_varchar_length(&self) -> u64 {
        self.max_varchar_length
    }

    pub fn identifier_quote_string(&self) -> &str {
        &self.identifier_quote_string
    }

    pub fn timezone(&self) -> &str {
        &self.time_zone
    }

    pub fn time_zone_behavior(&self) -> &str {
        &self.time_zone_behavior
    }
}
