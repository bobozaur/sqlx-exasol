#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![doc = include_str!("lib.md")]

pub use sqlx_a_orig::*;
pub use sqlx_exasol_impl::*;

/// Prevent re-exporting other drivers if used alongside `sqlx`.
mod postgres {}
mod sqlite {}
mod mysql {}

pub mod any {
    pub use sqlx_a_orig::any::*;
    pub use sqlx_exasol_impl::any::DRIVER;
}

#[cfg(any(feature = "derive", feature = "macros"))]
#[doc(hidden)]
pub extern crate sqlx_exasol_macros;

#[cfg(feature = "macros")]
pub use sqlx_exasol_macros::test;
#[cfg(feature = "derive")]
#[doc(hidden)]
pub use sqlx_exasol_macros::{FromRow, Type};

#[cfg(feature = "macros")]
mod macros;

#[cfg(feature = "macros")]
#[doc(hidden)]
pub mod ty_match;

#[doc = include_str!("types.md")]
pub mod types {
    pub use sqlx_a_orig::types::*;
    pub use sqlx_exasol_impl::types::*;

    #[cfg(feature = "chrono")]
    pub mod chrono {
        pub use sqlx_a_orig::types::chrono::*;
        pub use sqlx_exasol_impl::types::chrono::*;
    }

    #[cfg(feature = "time")]
    pub mod time {
        pub use sqlx_a_orig::types::time::*;
        pub use sqlx_exasol_impl::types::time::*;
    }

    #[cfg(feature = "derive")]
    #[doc(hidden)]
    pub use sqlx_exasol_macros::Type;
}

pub mod encode {
    pub use sqlx_a_orig::encode::*;
    #[cfg(feature = "derive")]
    #[doc(hidden)]
    pub use sqlx_exasol_macros::Encode;
}

pub mod decode {
    pub use sqlx_a_orig::decode::*;
    #[cfg(feature = "derive")]
    #[doc(hidden)]
    pub use sqlx_exasol_macros::Decode;
}
