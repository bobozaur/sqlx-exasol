#[allow(unused_imports)]
use sqlx_core::{config::drivers::Config, describe::Describe, impl_type_checking};
use sqlx_macros_core::{
    database::{CachingDescribeBlocking, DatabaseExt},
    query::QueryDriver,
};

use crate::{Exasol, SqlxResult};

pub const QUERY_DRIVER: QueryDriver = QueryDriver::new::<Exasol>();

impl DatabaseExt for Exasol {
    const DATABASE_PATH: &'static str = "sqlx_exasol::Exasol";

    const ROW_PATH: &'static str = "sqlx_exasol::ExaRow";

    fn describe_blocking(
        query: &str,
        database_url: &str,
        driver_config: &Config,
    ) -> SqlxResult<Describe<Self>> {
        static CACHE: CachingDescribeBlocking<Exasol> = CachingDescribeBlocking::new();

        CACHE.describe(query, database_url, driver_config)
    }
}

mod sqlx_exasol {
    #[allow(unused_imports, reason = "used in type checking")]
    pub mod types {
        pub use sqlx_core::types::*;

        pub use crate::types::*;

        #[cfg(feature = "chrono")]
        pub mod chrono {
            pub use sqlx_core::types::chrono::*;

            pub use crate::types::chrono::*;
        }

        #[cfg(feature = "time")]
        pub mod time {
            pub use sqlx_core::types::time::*;

            pub use crate::types::time::*;
        }
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

        sqlx_exasol::types::HashType,
        sqlx_exasol::types::ExaIntervalYearToMonth,

        #[cfg(feature = "uuid")]
        sqlx_exasol::types::Uuid,

        #[cfg(feature = "geo-types")]
        sqlx_exasol::types::geo_types::Geometry,
    },
    ParamChecking::Weak,
    feature-types: info => info.__type_feature_gate(),
    datetime-types: {
        chrono: {
            sqlx_exasol::types::chrono::TimeDelta,

            sqlx_exasol::types::chrono::NaiveDate,

            sqlx_exasol::types::chrono::NaiveDateTime,

            sqlx_exasol::types::chrono::DateTime<sqlx_exasol::types::chrono::Utc>,
        },
        time: {
            sqlx_exasol::types::time::Duration,

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
