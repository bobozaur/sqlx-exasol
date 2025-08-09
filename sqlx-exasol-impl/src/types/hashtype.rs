use serde::Deserialize;
use sqlx_core::{
    decode::Decode,
    encode::{Encode, IsNull},
    error::BoxDynError,
    types::Type,
};

use crate::{
    arguments::ExaBuffer,
    database::Exasol,
    type_info::{ExaDataType, ExaTypeInfo},
    value::ExaValueRef,
};

/// Newtype used for more explicit encoding/decoding of arbitrary length data into/from HASHTYPE
/// columns.
///
/// Unlike UUID, this type is not subject to length checks because Exasol can accept multiple
/// formats for these columns. While connections set the `HASHTYPE_FORMAT` database parameter to
/// `HEX` when they're opened to allow the driver to reliably use the reported column size for
/// length checks for UUIDs, this only affects the column output. Exasol will still accept any valid
/// input for the column, which can have different lengths depending on the data format.
///
/// See <https://docs.exasol.com/db/latest/sql_references/data_types/datatypedetails.htm#HASHTYPE>, in particular for your exact database version.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HashType(pub String);

impl Type<Exasol> for HashType {
    fn type_info() -> ExaTypeInfo {
        ExaDataType::HashType { size: None }.into()
    }
}

impl Encode<'_, Exasol> for HashType {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        <&str as Encode<Exasol>>::encode_by_ref(&self.0.as_str(), buf)
    }

    fn size_hint(&self) -> usize {
        <&str as Encode<Exasol>>::size_hint(&self.0.as_str())
    }
}

impl<'r> Decode<'r, Exasol> for HashType {
    fn decode(value: ExaValueRef<'r>) -> Result<Self, BoxDynError> {
        String::deserialize(value.value)
            .map(HashType)
            .map_err(From::from)
    }
}
