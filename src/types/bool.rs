use serde::Deserialize;
use sqlx::{encode::IsNull, error::BoxDynError, Decode, Encode, Type};

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
    fn encode_by_ref(&self, args: &mut Vec<String>) -> IsNull {
        let arg = serde_json::to_string(self).expect(concat!(
            "serializing primite ",
            stringify!(bool),
            " should work"
        ));
        args.push(arg);
        IsNull::No
    }
}

impl<'r> Decode<'r, Exasol> for bool {
    fn decode(value: ExaValueRef<'r>) -> Result<bool, BoxDynError> {
        bool::deserialize(&value.0.value).map_err(From::from)
    }
}
