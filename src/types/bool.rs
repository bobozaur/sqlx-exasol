use serde::Deserialize;
use serde_json::{json, Value};
use sqlx_core::{
    decode::Decode,
    encode::{Encode, IsNull},
    error::BoxDynError,
    types::Type,
};

use crate::{
    database::Exasol,
    type_info::{DataType, ExaTypeInfo},
    value::ExaValueRef,
};

impl Type<Exasol> for bool {
    fn type_info() -> ExaTypeInfo {
        ExaTypeInfo::new(DataType::Boolean)
    }
}

impl<'q> Encode<'q, Exasol> for bool {
    fn encode_by_ref(&self, args: &mut Vec<[Value; 1]>) -> IsNull {
        let arg = json!(self);
        args.push([arg]);
        IsNull::No
    }
}

impl<'r> Decode<'r, Exasol> for bool {
    fn decode(value: ExaValueRef<'r>) -> Result<bool, BoxDynError> {
        bool::deserialize(value.value).map_err(From::from)
    }
}
