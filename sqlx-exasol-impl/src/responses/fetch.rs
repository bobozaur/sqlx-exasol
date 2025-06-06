use serde::Deserialize;
use serde_json::Value;

use super::to_row_major;

/// Struct returned by doing [fetch](<https://github.com/exasol/websocket-api/blob/master/docs/commands/fetchV1.md>)
/// calls on a result set.
#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct DataChunk {
    pub num_rows: usize,
    #[serde(deserialize_with = "to_row_major")]
    pub data: Vec<Vec<Value>>,
}
