//! Command to generate offline query files:
//! ```shell
//! cargo run -p sqlx-exasol-cli prepare -- --features runtime-tokio --tests
//! ```

#[sqlx_exasol::test]
async fn test_query(
    mut conn: sqlx::pool::PoolConnection<sqlx_exasol::Exasol>,
) -> anyhow::Result<()> {
    let x: Option<String> = sqlx_exasol::query_scalar!("SELECT dummy FROM DUAL")
        .fetch_one(&mut *conn)
        .await?;

    Ok(())
}
