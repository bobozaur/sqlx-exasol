use base64::{engine::general_purpose::STANDARD as STD_BASE64_ENGINE, Engine};
use rand::rngs::OsRng;
use rsa::{Pkcs1v15Encrypt, RsaPublicKey};
use serde::Serialize;

use crate::error::ConResult;

/// Login type.
/// The variant chosen dictates which login process is called.
#[derive(Clone, Debug, Serialize)]
pub enum Login {
    Credentials(Credentials),
    AccessToken(AccessToken),
    RefreshToken(RefreshToken),
}

/// Login credentials.
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

#[derive(Clone, Debug, Serialize)]
pub(crate) enum LoginRef<'a> {
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

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CredentialsRef<'a> {
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

impl<'a> CredentialsRef<'a> {
    /// Encrypts the password with the provided key
    pub(crate) fn encrypt_password(&mut self, key: RsaPublicKey) -> ConResult<()> {
        let mut rng = OsRng;
        let padding = Pkcs1v15Encrypt;
        let pass_bytes = self.password.as_bytes();
        let enc_pass = key.encrypt(&mut rng, padding, pass_bytes)?;
        self.password = STD_BASE64_ENGINE.encode(enc_pass);
        Ok(())
    }
}
