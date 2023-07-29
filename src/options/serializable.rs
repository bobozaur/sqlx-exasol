use serde::Serialize;

use super::{login::LoginRef, ExaConnectOptionsRef};

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SerializableConOpts<'a> {
    #[serde(flatten)]
    login: LoginRef<'a>,
    client_name: &'static str,
    client_version: &'static str,
    client_os: &'static str,
    client_runtime: &'static str,
    use_compression: bool,
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
    const CLIENT_NAME: &str = "Rust Exasol";
}

impl<'a> From<ExaConnectOptionsRef<'a>> for SerializableConOpts<'a> {
    fn from(value: ExaConnectOptionsRef<'a>) -> Self {
        let crate_version = option_env!("CARGO_PKG_VERSION").unwrap_or("UNKNOWN");

        let attributes = Attributes {
            current_schema: value.schema,
            query_timeout: value.query_timeout,
            autocommit: true,
        };

        Self {
            login: value.login,
            client_name: Self::CLIENT_NAME,
            client_version: crate_version,
            client_os: std::env::consts::OS,
            use_compression: value.compression,
            client_runtime: Self::CLIENT_RUNTIME,
            attributes,
        }
    }
}