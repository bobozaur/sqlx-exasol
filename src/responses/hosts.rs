use serde::Deserialize;

/// Response returned from the database containing the IP's of its nodes.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Hosts {
    nodes: Vec<String>,
}

impl From<Hosts> for Vec<String> {
    fn from(value: Hosts) -> Self {
        value.nodes
    }
}
