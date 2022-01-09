use serde::de::{Error, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt::{Display, Formatter};

/// Enum listing the protocol versions that can be used when
/// establishing a websocket connection to Exasol.
/// Defaults to the highest defined protocol version and
/// falls back to the highest protocol version supported by the server.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ProtocolVersion {
    V1,
    V2,
    V3,
}

impl Display for ProtocolVersion {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ProtocolVersion::V1 => write!(f, "1"),
            ProtocolVersion::V2 => write!(f, "2"),
            ProtocolVersion::V3 => write!(f, "3"),
        }
    }
}

impl Serialize for ProtocolVersion {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::V1 => serializer.serialize_u64(1),
            Self::V2 => serializer.serialize_u64(2),
            Self::V3 => serializer.serialize_u64(3),
        }
    }
}

impl<'de> Deserialize<'de> for ProtocolVersion {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        /// Visitor for deserializing JSON into ProtocolVersion values.
        struct ProtocolVersionVisitor;

        impl<'de> Visitor<'de> for ProtocolVersionVisitor {
            type Value = ProtocolVersion;

            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                formatter
                    .write_str("Expecting an u8 representing the websocket API protocol version.")
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: Error,
            {
                match v {
                    1 => Ok(ProtocolVersion::V1),
                    2 => Ok(ProtocolVersion::V2),
                    3 => Ok(ProtocolVersion::V3),
                    _ => Err(E::custom("Unknown protocol version!")),
                }
            }
        }

        deserializer.deserialize_u64(ProtocolVersionVisitor)
    }
}
