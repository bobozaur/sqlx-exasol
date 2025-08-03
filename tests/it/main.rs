#![cfg(feature = "migrate")]

extern crate sqlx_exasol as sqlx;

#[cfg(feature = "bigdecimal")]
mod bigdecimal;
mod bool;
#[cfg(feature = "chrono")]
mod chrono;
mod common;
#[cfg(feature = "macros")]
mod compile_time;
mod describe;
mod error;
#[cfg(feature = "etl")]
mod etl;
mod float;
mod from_row;
mod geometry;
mod int;
mod interval_ytm;
mod invalid;
mod io;
#[cfg(feature = "json")]
mod json;
mod macros;
mod migrate;
#[cfg(feature = "rust_decimal")]
mod rust_decimal;
mod str;
mod test_attr;
#[cfg(feature = "time")]
mod time;
#[cfg(feature = "uuid")]
mod uuid;
