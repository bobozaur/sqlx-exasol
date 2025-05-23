use base64::{engine::general_purpose::STANDARD as STD_BASE64_ENGINE, Engine};
use rand::rngs::OsRng;
use rsa::{Pkcs1v15Encrypt, RsaPublicKey};
use serde::Serialize;
use sqlx_core::Error as SqlxError;

/// Enum representing the possible ways of authenticating a connection.
/// The variant chosen dictates which login process is called.
#[derive(Clone, Debug, Serialize)]
pub enum Login {
    Credentials(Credentials),
    AccessToken(AccessToken),
    RefreshToken(RefreshToken),
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Credentials {
    username: String,
    password: String,
}

impl Credentials {
    pub fn new(username: String, password: String) -> Self {
        Self { username, password }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AccessToken {
    access_token: String,
}

impl AccessToken {
    pub fn new(access_token: String) -> Self {
        Self { access_token }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RefreshToken {
    refresh_token: String,
}

impl RefreshToken {
    pub fn new(refresh_token: String) -> Self {
        Self { refresh_token }
    }
}

/// Serialization helper used particularly in the event
/// that we need to encrypt the password in a [`Login::Credentials`] login.
///
/// Because of that, the passowrd cannot be borrowed (since we'll mutate it),
/// hence no [`Copy`] derive.
#[derive(Clone, Debug, Serialize)]
#[serde(untagged)]
pub enum LoginRef<'a> {
    Credentials(CredentialsRef<'a>),
    AccessToken(&'a AccessToken),
    RefreshToken(&'a RefreshToken),
}

impl<'a> From<&'a Login> for LoginRef<'a> {
    fn from(value: &'a Login) -> Self {
        match value {
            Login::Credentials(c) => LoginRef::Credentials(c.into()),
            Login::AccessToken(a) => LoginRef::AccessToken(a),
            Login::RefreshToken(r) => LoginRef::RefreshToken(r),
        }
    }
}

/// Serialization helper. We need to encrypt the password,
/// hence mutate it, but the rest can be borrowed.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CredentialsRef<'a> {
    username: &'a str,
    password: String,
}

impl<'a> From<&'a Credentials> for CredentialsRef<'a> {
    fn from(value: &'a Credentials) -> Self {
        Self {
            username: &value.username,
            password: value.password.clone(),
        }
    }
}

impl CredentialsRef<'_> {
    /// Encrypts the password with the provided key.
    ///
    /// When connecting using [`Login::Credentials`], Exasol first sends out
    /// a public key to encrypt the password with.
    pub fn encrypt_password(&mut self, key: &RsaPublicKey) -> Result<(), SqlxError> {
        let mut rng = OsRng;

        let padding = Pkcs1v15Encrypt;
        let pass_bytes = self.password.as_bytes();

        let enc_pass = key
            .encrypt(&mut rng, padding, pass_bytes)
            .map_err(|e| SqlxError::Protocol(e.to_string()))?;

        self.password = STD_BASE64_ENGINE.encode(enc_pass);
        Ok(())
    }
}
