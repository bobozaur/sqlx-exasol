use serde::Deserialize;
use serde_json::Value;

/// Struct used for deserialization of fetched data
/// from getting a result set given a statement handle
#[derive(Debug, Deserialize)]
pub struct DataChunk {
    pub num_rows: usize,
    pub data: Vec<Vec<Value>>,
}
