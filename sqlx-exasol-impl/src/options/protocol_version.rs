use std::{
    fmt::{Display, Formatter},
    str::FromStr,
};

use serde::{Deserialize, Serialize};

use super::{error::ExaConfigError, PARAM_PROTOCOL_VERSION};

/// Enum listing the protocol versions that can be used when establishing a websocket connection to
/// Exasol. Defaults to the highest defined protocol version and falls back to the highest protocol
/// version supported by the server.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Deserialize, Serialize)]
#[serde(try_from = "u8")]
#[serde(into = "u8")]
#[repr(u8)]
pub enum ProtocolVersion {
    V1 = 1,
    V2 = 2,
    V3 = 3,
    V4 = 4,
}

impl FromStr for ProtocolVersion {
    type Err = ExaConfigError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "1" => Ok(ProtocolVersion::V1),
            "2" => Ok(ProtocolVersion::V2),
            "3" => Ok(ProtocolVersion::V3),
            "4" => Ok(ProtocolVersion::V4),
            _ => Err(ExaConfigError::InvalidParameter(PARAM_PROTOCOL_VERSION)),
        }
    }
}

impl From<ProtocolVersion> for u8 {
    fn from(value: ProtocolVersion) -> Self {
        value as Self
    }
}

impl TryFrom<u8> for ProtocolVersion {
    type Error = ExaConfigError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::V1),
            2 => Ok(Self::V2),
            3 => Ok(Self::V3),
            4 => Ok(Self::V4),
            _ => Err(ExaConfigError::InvalidParameter(PARAM_PROTOCOL_VERSION)),
        }
    }
}

impl Display for ProtocolVersion {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", *self as u8)
    }
}
