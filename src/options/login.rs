use std::borrow::Cow;

use base64::{engine::general_purpose::STANDARD as STD_BASE64_ENGINE, Engine};
use rand::rngs::OsRng;
use rsa::{Pkcs1v15Encrypt, RsaPublicKey};
use serde::Serialize;
use sqlx_core::Error as SqlxError;

/// Enum representing the possible ways of authenticating a connection.
/// The variant chosen dictates which login process is called.
#[derive(Clone, Debug)]
pub enum Login {
    Credentials { username: String, password: String },
    AccessToken { access_token: String },
    RefreshToken { refresh_token: String },
}

/// Serialization helper used particularly in the event that we need to encrypt the password in a
/// [`Login::Credentials`] login.
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

impl<'a> LoginRef<'a> {
    /// Encrypts the password with the provided key.
    ///
    /// When connecting using [`Login::Credentials`], Exasol first sends out
    /// a public key to encrypt the password with.
    pub fn encrypt_password(&mut self, key: &RsaPublicKey) -> Result<(), SqlxError> {
        let Self::Credentials { password, .. } = self else {
            return Ok(());
        };

        let enc_pass = key
            .encrypt(&mut OsRng, Pkcs1v15Encrypt, password.as_bytes())
            .map(|pass| STD_BASE64_ENGINE.encode(pass))
            .map(Cow::Owned)
            .map_err(|e| SqlxError::Protocol(e.to_string()))?;

        let _ = std::mem::replace(password, enc_pass);
        Ok(())
    }
}

impl<'a> From<&'a Login> for LoginRef<'a> {
    fn from(value: &'a Login) -> Self {
        match value {
            Login::Credentials { username, password } => LoginRef::Credentials {
                username,
                password: Cow::Borrowed(password),
            },
            Login::AccessToken { access_token } => LoginRef::AccessToken { access_token },
            Login::RefreshToken { refresh_token } => LoginRef::RefreshToken { refresh_token },
        }
    }
}
