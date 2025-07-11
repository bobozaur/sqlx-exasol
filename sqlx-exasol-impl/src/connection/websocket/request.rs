//! Module containing the various requests that can be sent to the Exasol server through its
//! WebSocket API.

use std::{borrow::Cow, num::NonZeroUsize, sync::Arc};

use base64::{engine::general_purpose::STANDARD as STD_BASE64_ENGINE, Engine};
use rsa::{Pkcs1v15Encrypt, RsaPublicKey};
use serde::{Serialize, Serializer};
use serde_json::value::RawValue;

use crate::{
    arguments::ExaBuffer, options::ProtocolVersion, responses::ExaRwAttributes, ExaAttributes,
    ExaTypeInfo, SqlxError, SqlxResult,
};

/// Serialization wrapper type that adds the read-write attributes to the database request if needed
/// and the request kind supports it.
pub struct WithAttributes<'attr, REQ> {
    needs_send: bool,
    attributes: &'attr ExaRwAttributes<'static>,
    request: &'attr mut REQ,
}

impl<'attr, REQ> WithAttributes<'attr, REQ> {
    pub fn new(request: &'attr mut REQ, attributes: &'attr ExaAttributes) -> Self {
        Self {
            needs_send: attributes.needs_send(),
            attributes: attributes.read_write(),
            request,
        }
    }
}

/// Request to login using credentials.
#[derive(Clone, Copy, Debug)]
pub struct LoginCreds(pub ProtocolVersion);

impl Serialize for WithAttributes<'_, LoginCreds> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let command = Command::Login {
            protocol_version: self.request.0,
        };

        command.serialize(serializer)
    }
}

/// Request to login using an access/refresh token.
#[derive(Clone, Copy, Debug)]
pub struct LoginToken(pub ProtocolVersion);

impl Serialize for WithAttributes<'_, LoginToken> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let command = Command::LoginToken {
            protocol_version: self.request.0,
        };

        command.serialize(serializer)
    }
}

/// Request to disconnect from the database.
#[derive(Clone, Copy, Debug, Default)]
pub struct Disconnect;

impl Serialize for WithAttributes<'_, Disconnect> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        Command::Disconnect.serialize(serializer)
    }
}

/// Request to fetch all read-write and read-only attributes for the connection.
#[derive(Clone, Copy, Debug, Default)]
pub struct GetAttributes;

impl Serialize for WithAttributes<'_, GetAttributes> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        Command::GetAttributes.serialize(serializer)
    }
}

/// Request to set the read-write attributes of the connection.
#[derive(Clone, Debug, Default)]
pub struct SetAttributes;

impl Serialize for WithAttributes<'_, SetAttributes> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let command = Command::SetAttributes {
            attributes: self.attributes,
        };

        command.serialize(serializer)
    }
}

/// Request to close an array of result sets.
#[derive(Clone, Debug)]
pub struct CloseResultSets(pub Vec<u16>);

impl Serialize for WithAttributes<'_, CloseResultSets> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let command = Command::CloseResultSet {
            attributes: self.needs_send.then_some(self.attributes),
            result_set_handles: &self.request.0,
        };

        command.serialize(serializer)
    }
}

/// Request to close a prepared statement.
#[derive(Copy, Clone, Debug)]
pub struct ClosePreparedStmt(pub u16);

impl Serialize for WithAttributes<'_, ClosePreparedStmt> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let command = Command::ClosePreparedStatement {
            attributes: self.needs_send.then_some(self.attributes),
            statement_handle: self.request.0,
        };

        command.serialize(serializer)
    }
}

/// Request to retrieve the IP addresses of all nodes in the Exasol cluster.
#[cfg(feature = "etl")]
#[derive(Clone, Copy, Debug)]
pub struct GetHosts(pub std::net::IpAddr);

#[cfg(feature = "etl")]
impl Serialize for WithAttributes<'_, GetHosts> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        Command::GetHosts {
            attributes: self.needs_send.then_some(self.attributes),
            host_ip: self.request.0,
        }
        .serialize(serializer)
    }
}

/// Request to fetch a data chunk for an open result set.
#[derive(Clone, Copy, Debug)]
pub struct Fetch {
    result_set_handle: u16,
    start_position: usize,
    num_bytes: usize,
}

impl Fetch {
    pub fn new(result_set_handle: u16, start_position: usize, num_bytes: usize) -> Self {
        Self {
            result_set_handle,
            start_position,
            num_bytes,
        }
    }
}

impl Serialize for WithAttributes<'_, Fetch> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let command = Command::Fetch {
            attributes: self.needs_send.then_some(self.attributes),
            result_set_handle: self.request.result_set_handle,
            start_position: self.request.start_position,
            num_bytes: self.request.num_bytes,
        };

        command.serialize(serializer)
    }
}

/// Request to execute a single SQL statement.
// This is internally used in the IMPORT/EXPORT jobs as well, since they rely on query execution too.
// However, in these scenarios the query is an owned string, hence the usage of [`Cow`] here to
// support that.
#[derive(Clone, Debug)]
pub struct Execute<'a>(pub Cow<'a, str>);

impl Serialize for WithAttributes<'_, Execute<'_>> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let command = Command::Execute {
            attributes: self.needs_send.then_some(self.attributes),
            sql_text: self.request.0.as_ref(),
        };

        command.serialize(serializer)
    }
}

/// Request to execute a batch of SQL statements.
#[derive(Clone, Debug)]
pub struct ExecuteBatch<'a>(pub Vec<&'a str>);

impl Serialize for WithAttributes<'_, ExecuteBatch<'_>> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let command = Command::ExecuteBatch {
            attributes: self.needs_send.then_some(self.attributes),
            sql_texts: &self.request.0,
        };

        command.serialize(serializer)
    }
}

/// Request to create a prepared statement.
#[derive(Clone, Debug)]
pub struct CreatePreparedStmt<'a>(pub &'a str);

impl Serialize for WithAttributes<'_, CreatePreparedStmt<'_>> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let command = Command::CreatePreparedStatement {
            attributes: self.needs_send.then_some(self.attributes),
            sql_text: self.request.0,
        };

        command.serialize(serializer)
    }
}

/// Request to execute a prepared statement.
#[derive(Clone, Debug)]
pub struct ExecutePreparedStmt {
    statement_handle: u16,
    num_columns: usize,
    num_rows: usize,
    columns: Arc<[ExaTypeInfo]>,
    data: PreparedStmtData,
}

impl ExecutePreparedStmt {
    pub fn new(handle: u16, columns: Arc<[ExaTypeInfo]>, data: ExaBuffer) -> Self {
        Self {
            statement_handle: handle,
            num_columns: columns.len(),
            num_rows: data.num_param_sets(),
            columns,
            data: data.into(),
        }
    }
}

impl Serialize for WithAttributes<'_, ExecutePreparedStmt> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let command = Command::ExecutePreparedStatement {
            attributes: self.needs_send.then_some(self.attributes),
            statement_handle: self.request.statement_handle,
            num_columns: self.request.num_columns,
            num_rows: self.request.num_rows,
            columns: &self.request.columns,
            data: &self.request.data,
        };

        command.serialize(serializer)
    }
}

/// A complete login request. This does not conform to the command structure and thus sits
/// separately.
///
/// Constructed from a reference of [`crate::ExaConnectOptions`]. The type borrows most of the data
/// from [`crate::ExaConnectOptions`], but uses a [`std::borrow::Cow`] for the password since it'll
/// get overwritten when encrypted.
///
/// This type is why [`ExaRwAttributes`] takes a lifetime parameter.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExaLoginRequest<'a> {
    #[serde(skip_serializing)]
    pub protocol_version: ProtocolVersion,
    #[serde(skip_serializing)]
    pub fetch_size: usize,
    #[serde(skip_serializing)]
    pub statement_cache_capacity: NonZeroUsize,
    #[serde(flatten)]
    pub login: LoginRef<'a>,
    pub use_compression: bool,
    pub client_name: &'static str,
    pub client_version: &'static str,
    pub client_os: &'static str,
    pub client_runtime: &'static str,
    pub attributes: ExaRwAttributes<'a>,
}

impl ExaLoginRequest<'_> {
    /// Encrypts the password with the provided key.
    ///
    /// When connecting using [`Login::Credentials`], Exasol first sends out a public key to encrypt
    /// the password with.
    pub fn encrypt_password(&mut self, key: &RsaPublicKey) -> SqlxResult<()> {
        let LoginRef::Credentials { password, .. } = &mut self.login else {
            return Ok(());
        };

        let enc_pass = key
            .encrypt(
                &mut rand::thread_rng(),
                Pkcs1v15Encrypt,
                password.as_bytes(),
            )
            .map(|pass| STD_BASE64_ENGINE.encode(pass))
            .map(Cow::Owned)
            .map_err(|e| SqlxError::Protocol(e.to_string()))?;

        let _ = std::mem::replace(password, enc_pass);
        Ok(())
    }
}

impl Serialize for WithAttributes<'_, ExaLoginRequest<'_>> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.request.serialize(serializer)
    }
}

/// Borrowed equivalent of [`crate::options::Login`], with the password wrapped in a
/// [`std::borrow::Cow`] as it'll get overwritten when encrypted.
#[derive(Clone, Debug, Serialize)]
#[serde(untagged)]
pub enum LoginRef<'a> {
    #[serde(rename_all = "camelCase")]
    Credentials {
        username: &'a str,
        password: Cow<'a, str>,
    },
    #[serde(rename_all = "camelCase")]
    AccessToken { access_token: &'a str },
    #[serde(rename_all = "camelCase")]
    RefreshToken { refresh_token: &'a str },
}

/// Serialization helper encapsulating all the commands that can be sent as a request.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
#[serde(tag = "command")]
enum Command<'a> {
    #[serde(rename_all = "camelCase")]
    Login {
        protocol_version: ProtocolVersion,
    },
    #[serde(rename_all = "camelCase")]
    LoginToken {
        protocol_version: ProtocolVersion,
    },
    Disconnect,
    GetAttributes,
    #[serde(rename_all = "camelCase")]
    SetAttributes {
        attributes: &'a ExaRwAttributes<'static>,
    },
    #[serde(rename_all = "camelCase")]
    CloseResultSet {
        #[serde(skip_serializing_if = "Option::is_none")]
        attributes: Option<&'a ExaRwAttributes<'static>>,
        result_set_handles: &'a [u16],
    },
    #[serde(rename_all = "camelCase")]
    ClosePreparedStatement {
        #[serde(skip_serializing_if = "Option::is_none")]
        attributes: Option<&'a ExaRwAttributes<'static>>,
        statement_handle: u16,
    },
    #[cfg(feature = "etl")]
    #[serde(rename_all = "camelCase")]
    GetHosts {
        #[serde(skip_serializing_if = "Option::is_none")]
        attributes: Option<&'a ExaRwAttributes<'static>>,
        host_ip: std::net::IpAddr,
    },
    #[serde(rename_all = "camelCase")]
    Fetch {
        #[serde(skip_serializing_if = "Option::is_none")]
        attributes: Option<&'a ExaRwAttributes<'static>>,
        result_set_handle: u16,
        start_position: usize,
        num_bytes: usize,
    },
    #[serde(rename_all = "camelCase")]
    Execute {
        #[serde(skip_serializing_if = "Option::is_none")]
        attributes: Option<&'a ExaRwAttributes<'static>>,
        sql_text: &'a str,
    },
    #[serde(rename_all = "camelCase")]
    ExecuteBatch {
        #[serde(skip_serializing_if = "Option::is_none")]
        attributes: Option<&'a ExaRwAttributes<'static>>,
        sql_texts: &'a [&'a str],
    },
    #[serde(rename_all = "camelCase")]
    CreatePreparedStatement {
        #[serde(skip_serializing_if = "Option::is_none")]
        attributes: Option<&'a ExaRwAttributes<'static>>,
        sql_text: &'a str,
    },
    #[serde(rename_all = "camelCase")]
    ExecutePreparedStatement {
        #[serde(skip_serializing_if = "Option::is_none")]
        attributes: Option<&'a ExaRwAttributes<'static>>,
        statement_handle: u16,
        num_columns: usize,
        num_rows: usize,
        #[serde(skip_serializing_if = "<[ExaTypeInfo]>::is_empty")]
        columns: &'a [ExaTypeInfo],
        #[serde(skip_serializing_if = "PreparedStmtData::is_empty")]
        data: &'a PreparedStmtData,
    },
}

/// Type containing the parameters data to be passed as part of executing a prepared statement.
/// It ensures the parameter sequence in the [`ExaBuffer`] is appropriately ended.
#[derive(Debug, Clone)]
struct PreparedStmtData {
    buffer: String,
    num_rows: usize,
}

impl PreparedStmtData {
    fn is_empty(&self) -> bool {
        self.num_rows == 0
    }
}

impl Serialize for PreparedStmtData {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // SAFETY: We are guaranteed that the buffer only contains valid JSON.
        //         The `transmute` is exactly how `serde_json` converts a str to a [`RawValue`] as
        //         well, and the benefit is that we do not go through the `serde` machinery to
        //         deserialize into a raw value just to serialize it right after.
        #[allow(clippy::transmute_ptr_to_ptr)]
        unsafe { std::mem::transmute::<&str, &RawValue>(&self.buffer) }.serialize(serializer)
    }
}

impl From<ExaBuffer> for PreparedStmtData {
    fn from(value: ExaBuffer) -> Self {
        Self {
            num_rows: value.num_param_sets(),
            buffer: value.finish(),
        }
    }
}
