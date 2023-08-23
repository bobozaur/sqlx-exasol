use std::iter;

use exasol::{ExaExport, ExaImport, Exasol, ExportOptions, ImportOptions, QueryOrTable};
use futures_util::{
    future::{try_join3, try_join_all},
    AsyncReadExt, AsyncWriteExt, TryFutureExt,
};
use sqlx::{Executor, Pool};
use sqlx_core::error::BoxDynError;

async fn pipe(mut reader: ExaExport, mut writer: ExaImport) -> Result<(), BoxDynError> {
    let mut buf = String::new();
    reader.read_to_string(&mut buf).await?;
    writer.write_all(buf.as_bytes()).await?;
    Ok(())
}

#[sqlx::test]
async fn test_http_transport_roundtrip_single_threaded(pool: Pool<Exasol>) -> anyhow::Result<()> {
    let mut conn1 = pool.acquire().await?;
    let mut conn2 = pool.acquire().await?;

    conn1
        .execute("CREATE TABLE TEST_EXPORT ( col VARCHAR(200) );")
        .await?;

    sqlx::query("INSERT INTO TEST_EXPORT VALUES (?)")
        .bind(vec!["dummy"; 1000])
        .execute(&mut *conn1)
        .await?;

    let (export_fut, readers) = ExportOptions::new(QueryOrTable::Table("TEST_EXPORT"))
        .execute(&mut conn1)
        .await?;

    let (import_fut, writers) = ImportOptions::new("TEST_EXPORT")
        .skip(1)
        .execute(&mut conn2)
        .await?;

    let transport_futs = iter::zip(readers, writers).map(|(r, w)| pipe(r, w));

    let (export_res, import_res, _) =
        try_join3(export_fut, import_fut, try_join_all(transport_futs))
            .await
            .map_err(|e| anyhow::anyhow! {e})?;

    assert_eq!(1000, export_res.rows_affected());
    assert_eq!(0, import_res.rows_affected());

    Ok(())
}

#[sqlx::test]
async fn test_http_transport_roundtrip_multi_threaded(pool: Pool<Exasol>) -> anyhow::Result<()> {
    let mut conn1 = pool.acquire().await?;
    let mut conn2 = pool.acquire().await?;

    conn1
        .execute("CREATE TABLE TEST_EXPORT ( col VARCHAR(200) );")
        .await?;

    sqlx::query("INSERT INTO TEST_EXPORT VALUES (?)")
        .bind(vec!["dummy"; 1000])
        .execute(&mut *conn1)
        .await?;

    let (export_fut, readers) = ExportOptions::new(QueryOrTable::Table("TEST_EXPORT"))
        .with_column_names(false)
        .execute(&mut conn1)
        .await?;

    let (import_fut, writers) = ImportOptions::new("TEST_EXPORT")
        .execute(&mut conn2)
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

    assert_eq!(1000, export_res.rows_affected());
    assert_eq!(0, import_res.rows_affected());

    Ok(())
}

// #[cfg(feature = "compression")]
// #[sqlx::test]
// async fn test_http_transport_roundtrip_compressed(pool: Pool<Exasol>) -> anyhow::Result<()> {
//     let mut conn1 = pool.acquire().await?;
//     let mut conn2 = pool.acquire().await?;

//     conn1
//         .execute("CREATE TABLE TEST_EXPORT ( col VARCHAR(200) );")
//         .await?;

//     sqlx::query("INSERT INTO TEST_EXPORT VALUES (?)")
//         .bind(vec!["dummy"; 1000])
//         .execute(&mut *conn1)
//         .await?;

//     let (export_fut, readers) = ExportOptions::new(QueryOrTable::Table("TEST_EXPORT"))
//         .compression(true)
//         .execute(&mut conn1)
//         .await?;

//     let (import_fut, writers) = ImportOptions::new("TEST_EXPORT")
//         .skip(1)
//         .compression(true)
//         .execute(&mut conn2)
//         .await?;

//     let transport_futs = iter::zip(readers, writers).map(|(r, w)| pipe(r, w));

//     env_logger::init();
//     let (export_res, import_res, _) =
//         try_join3(export_fut, import_fut, try_join_all(transport_futs))
//             .await
//             .map_err(|e| anyhow::anyhow! {e})?;

//     assert_eq!(1000, export_res.rows_affected());
//     assert_eq!(0, import_res.rows_affected());

//     Ok(())
// }
