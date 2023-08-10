mod bool;
#[cfg(feature = "chrono")]
mod chrono;
mod float;
mod int;
mod iter;
mod option;
#[cfg(feature = "rust_decimal")]
mod rust_decimal;
mod str;
mod uint;
#[cfg(feature = "uuid")]
mod uuid;

pub use iter::ExaIter;

const MIN_I64_NUMERIC: i64 = -999999999999999999;
const MAX_I64_NUMERIC: i64 = 1000000000000000000;
