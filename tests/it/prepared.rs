use exasol::{ExaConnectOptions, Exasol};
use sqlx_core::pool::PoolOptions;

#[cfg(feature = "migrate")]
#[sqlx::test]
async fn test_prepared_statement(pool_opts: PoolOptions<Exasol>, exa_opts: ExaConnectOptions) {}
