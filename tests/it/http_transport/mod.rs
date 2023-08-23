use exasol::{ExaExport, Exasol, ExportOptions, QueryOrTable};
use futures_util::{
    future::{try_join, try_join_all},
    AsyncReadExt, TryFutureExt,
};
use sqlx::{Executor, Pool};
use sqlx_core::error::BoxDynError;

#[sqlx::test]
async fn test_export_single_threaded(pool: Pool<Exasol>) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;

    conn.execute("CREATE TABLE TEST_EXPORT ( col VARCHAR(200) );")
        .await?;

    sqlx::query("INSERT INTO TEST_EXPORT VALUES (?)")
        .bind(vec!["blabla".to_owned(); 1000])
        .execute(&mut *conn)
        .await?;

    let (future, readers) = ExportOptions::new(QueryOrTable::Table("TEST_EXPORT"))
        .execute(&mut conn)
        .await?;

    let read_futures = readers.into_iter().map(read_data);

    let (query_res, _) = try_join(future, try_join_all(read_futures))
        .await
        .map_err(|e| anyhow::anyhow! {e})?;

    assert_eq!(1000, query_res.rows_affected());

    Ok(())
}

#[sqlx::test]
async fn test_export_multi_threaded(pool: Pool<Exasol>) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;

    conn.execute("CREATE TABLE TEST_EXPORT ( col VARCHAR(200) );")
        .await?;

    sqlx::query("INSERT INTO TEST_EXPORT VALUES (?)")
        .bind(vec!["blabla".to_owned(); 1000])
        .execute(&mut *conn)
        .await?;

    let (future, readers) = ExportOptions::new(QueryOrTable::Table("TEST_EXPORT"))
        .execute(&mut conn)
        .await?;

    let read_futures = readers.into_iter().map(|r| {
        tokio::spawn(read_data(r))
            .map_err(From::from)
            .and_then(|r| async { r })
    });

    let (query_res, _) = try_join(future, try_join_all(read_futures))
        .await
        .map_err(|e| anyhow::anyhow! {e})?;

    assert_eq!(1000, query_res.rows_affected());

    Ok(())
}

async fn read_data(mut reader: ExaExport) -> Result<(), BoxDynError> {
    let mut buf = String::new();
    reader.read_to_string(&mut buf).await?;
    Ok(())
}
