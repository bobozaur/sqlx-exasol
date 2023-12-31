use std::{
    fmt::{Display, Formatter},
    str::FromStr,
};

use serde::{de::Error, Deserialize, Deserializer, Serialize, Serializer};

use super::{error::ExaConfigError, PARAM_PROTOCOL_VERSION};

/// Enum listing the protocol versions that can be used when establishing a websocket connection to
/// Exasol. Defaults to the highest defined protocol version and falls back to the highest protocol
/// version supported by the server.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum ProtocolVersion {
    V1 = 1,
    V2 = 2,
    V3 = 3,
}

impl FromStr for ProtocolVersion {
    type Err = ExaConfigError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "1" => Ok(ProtocolVersion::V1),
            "2" => Ok(ProtocolVersion::V2),
            "3" => Ok(ProtocolVersion::V3),
            _ => Err(ExaConfigError::InvalidParameter(PARAM_PROTOCOL_VERSION)),
        }
    }
}

impl TryFrom<u8> for ProtocolVersion {
    type Error = ExaConfigError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::V1),
            2 => Ok(Self::V2),
            3 => Ok(Self::V3),
            _ => Err(ExaConfigError::InvalidParameter(PARAM_PROTOCOL_VERSION)),
        }
    }
}

impl Display for ProtocolVersion {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", *self as u8)
    }
}

impl Serialize for ProtocolVersion {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let val = *self as u8;
        val.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ProtocolVersion {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let val = u8::deserialize(deserializer)?;
        val.try_into().map_err(D::Error::custom)
    }
}
