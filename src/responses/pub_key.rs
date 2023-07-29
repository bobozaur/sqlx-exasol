use serde::Deserialize;

/// Struct representing public key information
/// returned as part of the login process.
#[allow(unused)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicKey {
    public_key_exponent: String,
    public_key_modulus: String,
    pub(crate) public_key_pem: String,
}
