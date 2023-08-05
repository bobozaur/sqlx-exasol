mod array;
mod bool;
#[cfg(feature = "chrono")]
mod chrono;
mod float;
mod int;
mod option;
#[cfg(feature = "rust_decimal")]
mod rust_decimal;
mod str;
mod uint;
#[cfg(feature = "uuid")]
mod uuid;

use std::fmt::Arguments;

use serde::Serialize;

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

impl ExaParameter for Arguments<'_> {}

impl ExaParameter for () {}
