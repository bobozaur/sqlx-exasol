use std::net::IpAddr;

use serde::Deserialize;

/// Response returned from the database containing the IP's of its nodes.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Hosts {
    pub nodes: Vec<IpAddr>,
}
