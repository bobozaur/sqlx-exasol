use serde::Deserialize;
use serde_json::Value;

/// Struct returned by doing [fetch](<https://github.com/exasol/websocket-api/blob/master/docs/commands/fetchV1.md>) 
/// calls on a result set.
#[derive(Debug, Deserialize)]
pub struct DataChunk {
    pub num_rows: usize,
    pub data: Vec<Vec<Value>>,
}
