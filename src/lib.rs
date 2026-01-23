#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc = include_str!("lib.md")]

pub use sqlx_core::acquire::Acquire;
pub use sqlx_core::arguments::{Arguments, IntoArguments};
pub use sqlx_core::column::Column;
pub use sqlx_core::column::ColumnIndex;
pub use sqlx_core::column::ColumnOrigin;
pub use sqlx_core::connection::{ConnectOptions, Connection};
pub use sqlx_core::database::{self, Database};
pub use sqlx_core::describe::Describe;
pub use sqlx_core::executor::{Execute, Executor};
pub use sqlx_core::from_row::FromRow;
pub use sqlx_core::pool::{self, Pool};
#[doc(hidden)]
pub use sqlx_core::query::query_with_result as __query_with_result;
pub use sqlx_core::query::{query, query_with};
pub use sqlx_core::query_as::{query_as, query_as_with};
pub use sqlx_core::query_builder::{self, QueryBuilder};
#[doc(hidden)]
pub use sqlx_core::query_scalar::query_scalar_with_result as __query_scalar_with_result;
pub use sqlx_core::query_scalar::{query_scalar, query_scalar_with};
#[rustfmt::skip] // preserver order
pub use sqlx_core::raw_sql::{raw_sql, RawSql};
pub use sqlx_core::row::Row;
pub use sqlx_core::sql_str::{AssertSqlSafe, SqlSafeStr, SqlStr};
pub use sqlx_core::statement::Statement;
pub use sqlx_core::transaction::Transaction;
pub use sqlx_core::type_info::TypeInfo;
pub use sqlx_core::types::Type;
pub use sqlx_core::value::{Value, ValueRef};
#[rustfmt::skip] // preserver order
pub use sqlx_core::Either;

#[doc(inline)]
pub use sqlx_core::error::{self, Error, Result};

#[cfg(feature = "migrate")]
pub use sqlx_core::migrate;

// Driver re-exports
#[doc(inline)]
pub use sqlx_exasol_impl::*;

#[rustfmt::skip] // preserver order
#[cfg(feature = "any")]
#[cfg_attr(docsrs, doc(cfg(feature = "any")))]
pub use crate::any::{reexports::*, Any, AnyExecutor};

#[cfg(any(feature = "derive", feature = "macros"))]
#[doc(hidden)]
pub extern crate sqlx_exasol_macros;

// derives
#[cfg(feature = "derive")]
#[doc(hidden)]
pub use sqlx_exasol_macros::{FromRow, Type};

#[cfg(feature = "macros")]
pub use sqlx_exasol_macros::test;

#[doc(hidden)]
#[cfg(feature = "migrate")]
pub use sqlx_core::testing;

#[doc(hidden)]
pub use sqlx_core::rt::test_block_on;

// Replicates `sqlx::any` module re-exports
#[cfg(feature = "any")]
pub mod any {
    #[allow(deprecated)]
    pub use sqlx_core::any::AnyKind;
    #[cfg_attr(docsrs, doc(cfg(feature = "any")))]
    pub use sqlx_core::any::{
        Any, AnyArguments, AnyConnectOptions, AnyExecutor, AnyPoolOptions, AnyQueryResult, AnyRow,
        AnyStatement, AnyTransactionManager, AnyTypeInfo, AnyTypeInfoKind, AnyValue, AnyValueRef,
        driver::install_drivers,
    };

    pub(crate) mod reexports {
        pub use sqlx_core::any::AnyConnection;
        pub use sqlx_core::any::AnyPool;
    }
    pub use sqlx_exasol_impl::any::*;
}

#[cfg(feature = "macros")]
mod macros;

// macro support
#[cfg(feature = "macros")]
#[doc(hidden)]
pub mod ty_match;

// Not present in `sqlx-core`, unfortunately.
#[cfg(any(feature = "derive", feature = "macros"))]
#[doc(hidden)]
pub use sqlx_orig::spec_error;

#[doc(hidden)]
pub use sqlx_core::rt as __rt;

#[doc = include_str!("types.md")]
pub mod types {
    pub use sqlx_core::types::*;
    pub use sqlx_exasol_impl::types::*;

    #[cfg(feature = "chrono")]
    pub mod chrono {
        pub use sqlx_core::types::chrono::*;
        pub use sqlx_exasol_impl::types::chrono::*;
    }

    #[cfg(feature = "time")]
    pub mod time {
        pub use sqlx_core::types::time::*;
        pub use sqlx_exasol_impl::types::time::*;
    }

    #[cfg(feature = "derive")]
    #[doc(hidden)]
    pub use sqlx_exasol_macros::Type;
}

/// Provides [`Encode`] for encoding values for the database.
pub mod encode {
    pub use sqlx_core::encode::{Encode, IsNull};

    #[cfg(feature = "derive")]
    #[doc(hidden)]
    pub use sqlx_exasol_macros::Encode;
}

pub use self::encode::Encode;

/// Provides [`Decode`] for decoding values from the database.
pub mod decode {
    pub use sqlx_core::decode::Decode;

    #[cfg(feature = "derive")]
    #[doc(hidden)]
    pub use sqlx_exasol_macros::Decode;
}

pub use self::decode::Decode;

/// Types and traits for the `query` family of functions and macros.
pub mod query {
    pub use sqlx_core::query::{Map, Query};
    pub use sqlx_core::query_as::QueryAs;
    pub use sqlx_core::query_scalar::QueryScalar;
}

/// Convenience re-export of common traits.
pub mod prelude {
    pub use super::Acquire;
    pub use super::ConnectOptions;
    pub use super::Connection;
    pub use super::Decode;
    pub use super::Encode;
    pub use super::Executor;
    pub use super::FromRow;
    pub use super::IntoArguments;
    pub use super::Row;
    pub use super::Statement;
    pub use super::Type;
}

#[cfg(feature = "_unstable-docs")]
pub use sqlx_core::config as _config;

// NOTE: APIs exported in this module are SemVer-exempt.
#[doc(hidden)]
pub mod _unstable {
    pub use sqlx_core::config;
}

// Not present in `sqlx-core`, unfortunately.
#[doc(hidden)]
pub use sqlx_orig::{
    warn_on_ambiguous_inferred_date_time_crate, warn_on_ambiguous_inferred_numeric_crate,
};
