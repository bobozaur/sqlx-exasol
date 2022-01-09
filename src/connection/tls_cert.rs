#[cfg(feature = "native-tls-basic")]
use __native_tls::Certificate as NtlsCert;
#[cfg(feature = "rustls")]
use __rustls::Certificate as RlsCert;

/// Wrapper over TLS certificate types provided by rustls and native-tls.
/// Used in fingerprint validation.
#[non_exhaustive]
pub enum Certificate {
    #[cfg(feature = "rustls")]
    RustlsCert(RlsCert),
    #[cfg(feature = "native-tls-basic")]
    NativeTlsCert(NtlsCert),
}

impl Certificate {
    #[allow(unreachable_patterns)]
    pub fn as_bytes(&self) -> Option<Vec<u8>> {
        match self {
            #[cfg(feature = "rustls")]
            Self::RustlsCert(cert) => Some(cert.as_ref().to_vec()),
            #[cfg(feature = "native-tls-basic")]
            Self::NativeTlsCert(cert) => cert.to_der().ok(),
            _ => None,
        }
    }
}
