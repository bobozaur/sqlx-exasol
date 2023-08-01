mod bool;
#[cfg(feature = "chrono")]
mod chrono;
mod float;
mod int;
#[cfg(feature = "rust_decimal")]
mod rust_decimal;
mod str;
mod uint;
#[cfg(feature = "uuid")]
mod uuid;

use serde_json::Value;
use sqlx_core::{
    database::{Database, HasArguments},
    encode::{Encode, IsNull},
    types::Type,
};

use crate::database::Exasol;

const MIN_I64_NUMERIC: i64 = -999999999999999999;
const MAX_I64_NUMERIC: i64 = 1000000000000000000;

impl<'q, T> Encode<'q, Exasol> for Option<T>
where
    T: Encode<'q, Exasol> + Type<Exasol> + 'q,
{
    #[inline]
    fn produces(&self) -> Option<<Exasol as Database>::TypeInfo> {
        if let Some(v) = self {
            v.produces()
        } else {
            Some(T::type_info())
        }
    }

    #[inline]
    fn encode(self, buf: &mut <Exasol as HasArguments<'q>>::ArgumentBuffer) -> IsNull {
        if let Some(v) = self {
            v.encode(buf)
        } else {
            buf.push([Value::Null]);
            IsNull::Yes
        }
    }

    #[inline]
    fn encode_by_ref(&self, buf: &mut <Exasol as HasArguments<'q>>::ArgumentBuffer) -> IsNull {
        if let Some(v) = self {
            v.encode_by_ref(buf)
        } else {
            buf.push([Value::Null]);
            IsNull::Yes
        }
    }

    #[inline]
    fn size_hint(&self) -> usize {
        self.as_ref().map_or(0, Encode::size_hint)
    }
}
