use super::{Attributes, ReqResult, Response, ResponseData};
use super::{Certificate, ConResult};
use crate::con_opts::LoginKind;
use crate::error::{ConnectionError, DriverError, Error, Result};
use crate::ConOpts;
use compress::MaybeCompressedWs;
use log::{debug, error, trace};
use rsa::pkcs1::DecodeRsaPublicKey;
use rsa::RsaPublicKey;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fmt::Write;
use std::net::TcpStream;
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{Message, WebSocket};

pub mod compress;

/// Websocket wrapper.
/// Mainly serves in separation of concerns, as it handles the lower level websocket calls.
pub struct ExaWebSocket {
    ws: MaybeCompressedWs,
    exa_attr: HashMap<String, Value>,
}

impl ExaWebSocket {
    pub fn new(ws: WebSocket<MaybeTlsStream<TcpStream>>, opts: ConOpts) -> Result<Self> {
        let mut exa_ws = Self {
            ws: MaybeCompressedWs::Plain(ws),
            exa_attr: HashMap::new(),
        };

        // Get compression flag before consuming opts
        let compression = opts.compression();

        // Login must always be uncompressed
        exa_ws.login(opts)?;

        // Enable compression if needed
        exa_ws.ws = exa_ws.ws.enable_compression(compression);

        Ok(exa_ws)
    }

    #[inline]
    pub fn read_message(&mut self) -> ReqResult<Message> {
        let msg = self.ws.as_inner_mut().read_message()?;
        Ok(msg)
    }

    #[inline]
    pub fn write_message(&mut self, msg: Message) -> ReqResult<()> {
        self.ws.as_inner_mut().write_message(msg)?;
        Ok(())
    }

    #[inline]
    pub fn close(&mut self) -> ReqResult<()> {
        self.ws.as_inner_mut().close(None)?;
        Ok(())
    }

    #[inline]
    pub fn can_write(&self) -> bool {
        self.ws.as_inner().can_write()
    }

    #[inline]
    pub fn exa_attr(&self) -> &HashMap<String, Value> {
        &self.exa_attr
    }

    /// Returns response data from a request.
    #[inline]
    pub fn get_resp_data(&mut self, payload: Value) -> Result<ResponseData> {
        self.do_request(payload)?
            .ok_or_else(|| DriverError::ResponseMismatch("response data").into())
    }

    /// Sends a request and waits for its response.
    pub fn do_request(&mut self, payload: Value) -> Result<Option<ResponseData>> {
        debug!("Sending websocket JSON request:\n{:#?}", &payload);

        let resp: Result<(Option<ResponseData>, Option<Attributes>)> = self
            .ws
            .send(payload)
            .and_then(|_| self.ws.recv())
            .map_err(DriverError::RequestError)
            .map_err(Error::DriverError)
            .and_then(|r| r.try_into());

        match &resp {
            Err(e) => error!("{:#?}", e),
            Ok((rd, attr)) => {
                if let Some(attr) = attr {
                    debug!("{:#?}", attr);
                }
                if let Some(rd) = rd {
                    match rd {
                        ResponseData::FetchedData(fd) => trace!("{:#?}", fd),
                        _ => debug!("{:#?}", rd),
                    }
                }
            }
        }

        let (data, attr) = resp?;
        if let Some(attributes) = attr {
            self.exa_attr.extend(attributes.map)
        }

        Ok(data)
    }

    /// Validates the DSN fingerprint.
    pub fn validate_fingerprint(&mut self, fingerprint: String) -> ConResult<()> {
        // The fingerprint is already a HEX string, so we just print it.
        let fingerprint = fingerprint.to_uppercase();
        let server_fp = self
            .peer_certificate()
            .and_then(|cert| cert.as_bytes())
            .map(|v| {
                let mut hasher = Sha256::new();
                hasher.update(v.as_slice());
                let mut output = String::with_capacity(fingerprint.len());
                for byte in hasher.finalize() {
                    write!(&mut output, "{:02X}", byte).ok()?
                }
                Some(output)
            })
            .and_then(|s| s);

        // If encryption is not enabled the server won't return a certificate,
        // so we just return Ok(()) in that case.
        match server_fp {
            None => Ok(()),
            Some(fp) => match fingerprint == fp {
                true => Ok(()),
                false => Err(ConnectionError::FingerprintMismatch(fingerprint, fp)),
            },
        }
    }

    /// Gets the peer leaf certificate
    fn peer_certificate(&mut self) -> Option<Certificate> {
        let stream = self.ws.as_inner_mut().get_ref();
        match stream {
            #[cfg(feature = "rustls")]
            MaybeTlsStream::Rustls(s) => s
                .conn
                .peer_certificates()
                .map(|certs| certs.first())
                .flatten()
                .map(|cert| Certificate::RustlsCert(cert.clone())),
            #[cfg(feature = "native-tls-basic")]
            MaybeTlsStream::NativeTls(s) => s
                .peer_certificate()
                .ok()
                .flatten()
                .map(|cert| Certificate::NativeTlsCert(cert)),
            _ => None,
        }
    }

    /// Gets the public key from Exasol
    /// Used during authentication for encrypting the password
    fn get_public_key(&mut self, opts: &ConOpts) -> Result<RsaPublicKey> {
        let payload = json!({"command": "login", "protocolVersion": opts.protocol_version()});
        let pem: String = self.get_resp_data(payload).and_then(|p| p.try_into())?;

        Ok(RsaPublicKey::from_pkcs1_pem(&pem)
            .map_err(ConnectionError::PKCS1Error)
            .map_err(DriverError::ConnectionError)?)
    }

    /// Authenticates to Exasol
    /// Called after the websocket is established
    ///
    /// Login is always uncompressed. If compression is enabled, it is set afterwards.
    fn login(&mut self, opts: ConOpts) -> Result<()> {
        match opts.login_kind() {
            LoginKind::Credentials(_) => self.login_creds(opts),
            _ => self.login_token(opts),
        }
    }

    /// Logs in with credentials (username and password).
    fn login_creds(&mut self, mut opts: ConOpts) -> Result<()> {
        let key = self.get_public_key(&opts)?;
        opts.encrypt_password(key)
            .map_err(DriverError::ConnectionError)?;

        let payload = json!(opts);
        self.do_request(payload)?;
        Ok(())
    }

    /// Logs in using a token.
    fn login_token(&mut self, opts: ConOpts) -> Result<()> {
        let payload = json!({"command": "loginToken", "protocolVersion": opts.protocol_version()});
        self.do_request(payload)?;

        let payload = json!(opts);
        self.do_request(payload)?;
        Ok(())
    }
}
