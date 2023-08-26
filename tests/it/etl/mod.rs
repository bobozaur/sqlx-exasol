use std::iter;

use exasol::{
    etl::{ExaExport, ExaImport, ExportBuilder, ImportBuilder, QueryOrTable},
    Exasol,
};
use futures_util::{
    future::{try_join3, try_join_all},
    AsyncReadExt, AsyncWriteExt, TryFutureExt,
};
use sqlx::{Executor, Pool};
use sqlx_core::error::BoxDynError;

const NUM_ROWS: usize = 1_000_000;

#[sqlx::test]
async fn test_http_transport_roundtrip_single_threaded(pool: Pool<Exasol>) -> anyhow::Result<()> {
    let mut conn1 = pool.acquire().await?;
    let mut conn2 = pool.acquire().await?;

    conn1
        .execute("CREATE TABLE TEST_HTTP_TRANSPORT ( col VARCHAR(200) );")
        .await?;

    sqlx::query("INSERT INTO TEST_HTTP_TRANSPORT VALUES (?)")
        .bind(vec!["dummy"; NUM_ROWS])
        .execute(&mut *conn1)
        .await?;

    let (export_fut, readers) = ExportBuilder::new(QueryOrTable::Table("TEST_HTTP_TRANSPORT"))
        .build(&mut conn1)
        .await?;

    let (import_fut, writers) = ImportBuilder::new("TEST_HTTP_TRANSPORT")
        .skip(1)
        .build(&mut conn2)
        .await?;

    let transport_futs = iter::zip(readers, writers).map(|(r, w)| pipe(r, w));

    let (export_res, import_res, _) =
        try_join3(export_fut, import_fut, try_join_all(transport_futs))
            .await
            .map_err(|e| anyhow::anyhow! {e})?;

    assert_eq!(NUM_ROWS as u64, export_res.rows_affected());
    assert_eq!(NUM_ROWS as u64, import_res.rows_affected());

    let num_rows: u64 = sqlx::query_scalar("SELECT COUNT(*) FROM TEST_HTTP_TRANSPORT")
        .fetch_one(&mut *conn1)
        .await?;

    assert_eq!(num_rows, 2 * NUM_ROWS as u64);

    Ok(())
}

#[sqlx::test]
async fn test_http_transport_roundtrip_multi_threaded(pool: Pool<Exasol>) -> anyhow::Result<()> {
    let mut conn1 = pool.acquire().await?;
    let mut conn2 = pool.acquire().await?;

    conn1
        .execute("CREATE TABLE TEST_HTTP_TRANSPORT ( col VARCHAR(200) );")
        .await?;

    sqlx::query("INSERT INTO TEST_HTTP_TRANSPORT VALUES (?)")
        .bind(vec!["dummy"; NUM_ROWS])
        .execute(&mut *conn1)
        .await?;

    let (export_fut, readers) = ExportBuilder::new(QueryOrTable::Table("TEST_HTTP_TRANSPORT"))
        .with_column_names(false)
        .build(&mut conn1)
        .await?;

    let (import_fut, writers) = ImportBuilder::new("TEST_HTTP_TRANSPORT")
        .buffer_size(2048)
        .build(&mut conn2)
        .await?;

    let transport_futs = iter::zip(readers, writers).map(|(r, w)| {
        tokio::spawn(pipe(r, w))
            .map_err(From::from)
            .and_then(|r| async { r })
    });

    let (export_res, import_res, _) =
        try_join3(export_fut, import_fut, try_join_all(transport_futs))
            .await
            .map_err(|e| anyhow::anyhow! {e})?;

    assert_eq!(NUM_ROWS as u64, export_res.rows_affected());
    assert_eq!(NUM_ROWS as u64, import_res.rows_affected());

    let num_rows: u64 = sqlx::query_scalar("SELECT COUNT(*) FROM TEST_HTTP_TRANSPORT")
        .fetch_one(&mut *conn1)
        .await?;

    assert_eq!(num_rows, 2 * NUM_ROWS as u64);

    Ok(())
}

#[cfg(feature = "compression")]
#[sqlx::test]
async fn test_http_transport_roundtrip_compressed(pool: Pool<Exasol>) -> anyhow::Result<()> {
    let mut conn1 = pool.acquire().await?;
    let mut conn2 = pool.acquire().await?;

    conn1
        .execute("CREATE TABLE TEST_HTTP_TRANSPORT ( col VARCHAR(200) );")
        .await?;

    sqlx::query("INSERT INTO TEST_HTTP_TRANSPORT VALUES (?)")
        .bind(vec!["dummy"; NUM_ROWS])
        .execute(&mut *conn1)
        .await?;

    let (export_fut, readers) = ExportBuilder::new(QueryOrTable::Table("TEST_HTTP_TRANSPORT"))
        .with_column_names(false)
        .compression(true)
        .build(&mut conn1)
        .await?;

    let (import_fut, writers) = ImportBuilder::new("TEST_HTTP_TRANSPORT")
        .compression(true)
        .build(&mut conn2)
        .await?;

    let transport_futs = iter::zip(readers, writers).map(|(r, w)| {
        tokio::spawn(pipe(r, w))
            .map_err(From::from)
            .and_then(|r| async { r })
    });

    let (export_res, import_res, _) =
        try_join3(export_fut, import_fut, try_join_all(transport_futs))
            .await
            .map_err(|e| anyhow::anyhow! {e})?;

    assert_eq!(NUM_ROWS as u64, export_res.rows_affected());
    assert_eq!(NUM_ROWS as u64, import_res.rows_affected());

    let num_rows: u64 = sqlx::query_scalar("SELECT COUNT(*) FROM TEST_HTTP_TRANSPORT")
        .fetch_one(&mut *conn1)
        .await?;

    assert_eq!(num_rows, 2 * NUM_ROWS as u64);

    Ok(())
}

async fn pipe(mut reader: ExaExport, mut writer: ExaImport) -> Result<(), BoxDynError> {
    let mut buf = String::new();
    reader.read_to_string(&mut buf).await?;
    writer.write_all(buf.as_bytes()).await?;
    writer.close().await?;
    Ok(())
}
