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

use std::fmt::Arguments;

use serde::Serialize;
use sqlx_core::{
    database::Database,
    encode::{Encode, IsNull},
    types::Type,
};

use crate::{arguments::ExaBuffer, database::Exasol};

const MIN_I64_NUMERIC: i64 = -999999999999999999;
const MAX_I64_NUMERIC: i64 = 1000000000000000000;

/// Trait for types that can be query parameters.
pub trait ExaParameter: Serialize {
    /// Method that tells the driver how many rows
    /// this type carries parameter for.
    ///
    /// If this is not a parameter set but rather a single parameter
    /// (which is most often th case), then [`None`] is expected.
    ///
    /// `sqlx` only encodes one value for one column at a time,
    /// and this method's default implementation reflects that.
    ///
    /// If you, however, implement this on some sort of container,
    /// you must override this and provide the correct output.
    fn parameter_set_len(&self) -> Option<usize> {
        None
    }
}

impl<T> ExaParameter for &T
where
    T: Serialize + ExaParameter,
{
    fn parameter_set_len(&self) -> Option<usize> {
        (**self).parameter_set_len()
    }
}

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
    fn encode(self, buf: &mut ExaBuffer) -> IsNull {
        if let Some(v) = self {
            v.encode(buf)
        } else {
            buf.append(());
            IsNull::Yes
        }
    }

    #[inline]
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> IsNull {
        if let Some(v) = self {
            v.encode_by_ref(buf)
        } else {
            buf.append(());
            IsNull::Yes
        }
    }

    #[inline]
    fn size_hint(&self) -> usize {
        self.as_ref().map_or(0, Encode::size_hint)
    }
}

impl<T> ExaParameter for Vec<T>
where
    T: Serialize,
{
    fn parameter_set_len(&self) -> Option<usize> {
        Some(self.len())
    }
}

impl<T> Type<Exasol> for Vec<T>
where
    T: Type<Exasol>,
{
    fn type_info() -> <Exasol as Database>::TypeInfo {
        T::type_info()
    }
}

impl<'q, T> Encode<'q, Exasol> for Vec<T>
where
    T: Encode<'q, Exasol> + Type<Exasol> + 'q + Serialize,
{
    fn produces(&self) -> Option<<Exasol as Database>::TypeInfo> {
        if let Some(v) = self.first() {
            v.produces()
        } else {
            Some(T::type_info())
        }
    }

    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> IsNull {
        buf.append(self);
        IsNull::No
    }

    fn size_hint(&self) -> usize {
        match self.first() {
            Some(v) => v.size_hint() * self.len(),
            None => 0,
        }
    }
}

impl ExaParameter for Arguments<'_> {}

impl ExaParameter for () {}
