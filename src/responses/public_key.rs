use rsa::{errors::Error as RsaError, pkcs1::DecodeRsaPublicKey, RsaPublicKey};
use serde::Deserialize;

use crate::{error::ToSqlxError, SqlxError};

/// The public key Exasol sends during the login process
/// to be used for encrypting the password.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(try_from = "PublicKeyDe")]
pub struct PublicKey(RsaPublicKey);

impl From<PublicKey> for RsaPublicKey {
    fn from(value: PublicKey) -> Self {
        value.0
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicKeyDe {
    public_key_pem: String,
}

impl TryFrom<PublicKeyDe> for PublicKey {
    type Error = SqlxError;

    fn try_from(value: PublicKeyDe) -> Result<Self, Self::Error> {
        let public_key = RsaPublicKey::from_pkcs1_pem(&value.public_key_pem)
            .map_err(RsaError::from)
            .map_err(ToSqlxError::to_sqlx_err)?;

        Ok(PublicKey(public_key))
    }
}
