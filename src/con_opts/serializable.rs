use serde::Serialize;

use super::{ExaConnectOptions, LoginKind, ProtocolVersion};

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SerializableConOpts<'a> {
    #[serde(flatten)]
    login_kind: &'a LoginKind,
    protocol_version: ProtocolVersion,
    client_name: &'a str,
    client_version: &'a str,
    client_os: &'a str,
    fetch_size: usize,
    use_encryption: bool,
    use_compression: bool,
    lowercase_columns: bool,
    client_runtime: &'static str,
    attributes: Attributes<'a>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Attributes<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    current_schema: Option<&'a str>,
    query_timeout: u64,
    autocommit: bool,
}

impl<'a> SerializableConOpts<'a> {
    const CLIENT_RUNTIME: &str = "Rust";
}

impl<'a> From<&'a ExaConnectOptions> for SerializableConOpts<'a> {
    fn from(value: &'a ExaConnectOptions) -> Self {
        let attributes = Attributes {
            current_schema: value.schema.as_deref(),
            query_timeout: value.query_timeout,
            autocommit: value.autocommit,
        };

        Self {
            login_kind: &value.login_kind,
            protocol_version: value.protocol_version,
            client_name: &value.client_name,
            client_version: &value.client_version,
            client_os: &value.client_os,
            fetch_size: value.fetch_size,
            use_encryption: value.use_encryption,
            use_compression: value.use_compression,
            lowercase_columns: value.lowercase_columns,
            client_runtime: Self::CLIENT_RUNTIME,
            attributes,
        }
    }
}
