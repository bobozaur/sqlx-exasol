#[allow(unused_imports)]
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

mod sqlx_exasol {
    pub mod types {
        #[allow(unused_imports, reason = "used in type checking")]
        pub use sqlx_core::types::*;

        pub use crate::types::*;
    }
}

impl_type_checking!(
    Exasol {        
        bool,
        i8,
        i16,
        i32,
        i64,
        f64,
        String | &str,
        sqlx_exasol::types::ExaIntervalYearToMonth,
        sqlx_exasol::types::ExaIntervalDayToSecond,
    },
    ParamChecking::Weak,
    feature-types: info => info.__type_feature_gate(),
    datetime-types: {
        chrono: {
            sqlx_exasol::types::chrono::NaiveDate,

            sqlx_exasol::types::chrono::NaiveDateTime,

            sqlx_exasol::types::chrono::DateTime<sqlx_exasol::types::chrono::Utc>,
        },
        time: {
            sqlx_exasol::types::time::Date,

            sqlx_exasol::types::time::PrimitiveDateTime,

            sqlx_exasol::types::time::OffsetDateTime,
        },
    },
    numeric-types: {
        bigdecimal: {
            sqlx_exasol::types::BigDecimal,
        },
        rust_decimal: {
            sqlx_exasol::types::Decimal,
        },
    },
);
