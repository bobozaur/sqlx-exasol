use super::ConResult;
use rand::rngs::OsRng;
use rsa::{PaddingScheme, PublicKey, RsaPublicKey};

/// Login credentials.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Credentials {
    username: String,
    password: String,
}

impl Credentials {
    pub fn new<T, U>(username: T, password: U) -> Self
    where
        T: Into<String>,
        U: Into<String>,
    {
        Self {
            username: username.into(),
            password: password.into(),
        }
    }

    pub fn username(&self) -> &str {
        self.username.as_str()
    }

    pub fn password(&self) -> &str {
        self.password.as_str()
    }

    /// Encrypts the password with the provided key
    pub(crate) fn encrypt_password(&mut self, key: RsaPublicKey) -> ConResult<()> {
        let mut rng = OsRng;
        let padding = PaddingScheme::new_pkcs1v15_encrypt();
        let pass_bytes = self.password.as_bytes();
        let enc_pass = key.encrypt(&mut rng, padding, pass_bytes)?;
        self.password = base64::encode(enc_pass);
        Ok(())
    }
}

/// Login type.
/// The variant chosen dictates which login process is called.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LoginKind {
    Credentials(Credentials),
    AccessToken(String),
    RefreshToken(String),
}

impl Default for LoginKind {
    fn default() -> Self {
        Self::Credentials(Credentials::default())
    }
}

impl LoginKind {
    pub(crate) fn encrypt_password(&mut self, key: RsaPublicKey) -> ConResult<()> {
        match self {
            Self::Credentials(creds) => creds.encrypt_password(key),
            _ => Ok(()),
        }
    }
}
