#[cfg(feature = "ascii")]
pub mod ascii;
#[cfg(feature = "bigdecimal")]
mod bigdecimal;
mod bool;
#[cfg(feature = "chrono")]
mod chrono;
mod float;
mod int;
mod interval;
mod iter;
#[cfg(feature = "json")]
mod json;
mod option;
#[cfg(feature = "rust_decimal")]
mod rust_decimal;
mod str;
mod text;
#[cfg(feature = "time")]
mod time;
#[cfg(feature = "uuid")]
mod uuid;

#[cfg(feature = "ascii")]
pub use ascii::{AsciiStr, AsciiString};
pub use interval::{ExaIntervalDayToSecond, ExaIntervalYearToMonth};
pub use iter::ExaIter;

/// Timestamps encode length used in size hints.
/// Represents:
///
/// 1 quote +
/// 4 years + 1 dash + 2 months + 1 dash + 2 days +
/// 1 space + 2 hours + 2 minutes + 2 seconds + 9 subseconds +
/// 1 quote
#[cfg(any(feature = "chrono", feature = "time"))]
const TS_ENC_LEN: usize = 1 + 4 + 1 + 2 + 1 + 2 + 1 + 2 + 2 + 2 + 9 + 1;
