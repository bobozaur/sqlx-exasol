#[allow(unused_imports)]
use sqlx_core as sqlx;
use sqlx_core::{describe::Describe, impl_type_checking};
use sqlx_macros_core::{
    database::{CachingDescribeBlocking, DatabaseExt},
    query::QueryDriver,
};

use crate::{Exasol, SqlxResult};

pub const QUERY_DRIVER: QueryDriver = QueryDriver::new::<Exasol>();

impl DatabaseExt for Exasol {
    const DATABASE_PATH: &'static str = "sqlx_exasol::Exasol";

    const ROW_PATH: &'static str = "sqlx_exasol::ExaRow";

    fn describe_blocking(query: &str, database_url: &str) -> SqlxResult<Describe<Self>> {
        static CACHE: CachingDescribeBlocking<Exasol> = CachingDescribeBlocking::new();

        CACHE.describe(query, database_url)
    }
}

impl_type_checking!(
    Exasol {
        bool,
        u8,
        u16,
        u32,
        u64,
        u128,
        i8,
        i16,
        i32,
        i64,
        i128,
        f32,
        f64,
        String,

        // External types
        #[cfg(feature = "chrono")]
        sqlx::types::chrono::NaiveDate,

        #[cfg(feature = "chrono")]
        sqlx::types::chrono::NaiveDateTime,

        #[cfg(feature = "chrono")]
        sqlx::types::chrono::DateTime<sqlx::types::chrono::Utc>,

        #[cfg(feature = "rust_decimal")]
        sqlx::types::Decimal,

        #[cfg(feature = "uuid")]
        sqlx::types::Uuid,
    },
    ParamChecking::Weak,
    feature-types: _info => None,
);
