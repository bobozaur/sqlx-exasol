use serde::Deserialize;

/// Struct representing the hosts of the Exasol cluster
#[allow(unused)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Hosts {
    num_nodes: usize,
    nodes: Vec<String>,
}