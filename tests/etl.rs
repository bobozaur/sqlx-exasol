#![cfg(all(feature = "migrate", feature = "etl"))]

mod macros;

use std::iter;

use futures_util::{
    future::{try_join, try_join3, try_join_all},
    AsyncReadExt, AsyncWriteExt, TryFutureExt,
};
use sqlx_exasol::{
    error::BoxDynError,
    etl::{ExaExport, ExaImport, ExportBuilder, ExportSource, ImportBuilder},
    pool::{PoolConnection, PoolOptions},
    Connection, ExaConnectOptions, Exasol, Executor,
};

const NUM_ROWS: usize = 1_000_000;

test_etl_single_threaded!(
    "simple",
    "TEST_ETL",
    ExportBuilder::new(ExportSource::Table("TEST_ETL")),
    ImportBuilder::new("TEST_ETL")
);

test_etl_multi_threaded!(
    "simple",
    "TEST_ETL",
    ExportBuilder::new(ExportSource::Table("TEST_ETL")),
    ImportBuilder::new("TEST_ETL")
);

test_etl_single_threaded!(
    "query_export",
    "TEST_ETL",
    ExportBuilder::new(ExportSource::Query("SELECT * FROM TEST_ETL")),
    ImportBuilder::new("TEST_ETL")
);

test_etl_single_threaded!(
    "multiple_workers",
    0,
    "TEST_ETL",
    ExportBuilder::new(ExportSource::Table("TEST_ETL")),
    ImportBuilder::new("TEST_ETL")
);

test_etl_multi_threaded!(
    "multiple_workers",
    0,
    "TEST_ETL",
    ExportBuilder::new(ExportSource::Table("TEST_ETL")),
    ImportBuilder::new("TEST_ETL")
);

test_etl_single_threaded!(
    "all_arguments",
    "TEST_ETL",
    ExportBuilder::new(ExportSource::Table("TEST_ETL"))
        .num_readers(1)
        .comment("test")
        .encoding("ASCII")
        .null("OH-NO")
        .row_separator(sqlx_exasol::etl::RowSeparator::LF)
        .column_separator("|")
        .column_delimiter("\\\\")
        .with_column_names(true),
    ImportBuilder::new("TEST_ETL")
        .skip(1)
        .buffer_size(20000)
        .columns(Some(&["col"]))
        .num_writers(1)
        .comment("test")
        .encoding("ASCII")
        .null("OH-NO")
        .row_separator(sqlx_exasol::etl::RowSeparator::LF)
        .column_separator("|")
        .column_delimiter("\\\\")
        .trim(sqlx_exasol::etl::Trim::Both)
);

test_etl!(
    "single_threaded",
    "writer_flush_first",
    1,
    "TEST_ETL",
    |(r, w)| pipe_flush_writers(r, w),
    ExportBuilder::new(ExportSource::Table("TEST_ETL")),
    ImportBuilder::new("TEST_ETL")
);

// ##########################################
// ################ Failures ################
// ##########################################
#[ignore]
#[sqlx_exasol::test]
async fn test_etl_invalid_query(mut conn: PoolConnection<Exasol>) -> Result<(), BoxDynError> {
    conn.execute("CREATE TABLE TEST_ETL ( col VARCHAR(200) );")
        .await?;

    sqlx_exasol::query("INSERT INTO TEST_ETL VALUES (?)")
        .bind(vec!["dummy"; NUM_ROWS])
        .execute(&mut *conn)
        .await?;

    let (export_fut, readers) = ExportBuilder::new(ExportSource::Table(";)BAD_TABLE_NAME*&"))
        .build(&mut conn)
        .await?;

    try_join(
        export_fut.map_err(From::from),
        try_join_all(readers.into_iter().map(read_data)),
    )
    .await
    .unwrap_err();

    Ok(())
}

#[ignore]
#[sqlx_exasol::test]
async fn test_etl_reader_drop(mut conn: PoolConnection<Exasol>) -> Result<(), BoxDynError> {
    conn.execute("CREATE TABLE TEST_ETL ( col VARCHAR(200) );")
        .await?;

    sqlx_exasol::query("INSERT INTO TEST_ETL VALUES (?)")
        .bind(vec!["dummy"; NUM_ROWS])
        .execute(&mut *conn)
        .await?;

    let (export_fut, readers) = ExportBuilder::new(ExportSource::Table("TEST_ETL"))
        .build(&mut conn)
        .await?;

    let transport_futs = readers
        .into_iter()
        .enumerate()
        .map(|(idx, r)| drop_some_readers(idx, r));

    try_join(export_fut.map_err(From::from), try_join_all(transport_futs))
        .await
        .unwrap_err();

    Ok(())
}

#[ignore]
#[sqlx_exasol::test]
async fn test_etl_transaction_import_rollback(
    mut conn: PoolConnection<Exasol>,
) -> Result<(), BoxDynError> {
    conn.execute("CREATE TABLE TEST_ETL ( col VARCHAR(200) );")
        .await?;

    sqlx_exasol::query("INSERT INTO TEST_ETL VALUES (?)")
        .bind(vec!["dummy"; NUM_ROWS])
        .execute(&mut *conn)
        .await?;

    let mut tx = conn.begin().await?;

    let (import_fut, writers) = ImportBuilder::new("TEST_ETL").build(&mut tx).await?;

    let transport_futs = writers.into_iter().map(write_one_row);

    try_join(import_fut.map_err(From::from), try_join_all(transport_futs)).await?;

    tx.rollback().await?;

    let num_rows: i64 = sqlx_exasol::query_scalar("SELECT COUNT(*) FROM TEST_ETL")
        .fetch_one(&mut *conn)
        .await?;

    assert_eq!(num_rows, NUM_ROWS as i64);

    Ok(())
}

#[ignore]
#[sqlx_exasol::test]
async fn test_etl_transaction_import_commit(
    mut conn: PoolConnection<Exasol>,
) -> Result<(), BoxDynError> {
    conn.execute("CREATE TABLE TEST_ETL ( col VARCHAR(200) );")
        .await?;

    sqlx_exasol::query("INSERT INTO TEST_ETL VALUES (?)")
        .bind(vec!["dummy"; NUM_ROWS])
        .execute(&mut *conn)
        .await?;

    let mut tx = conn.begin().await?;

    let (import_fut, writers) = ImportBuilder::new("TEST_ETL").build(&mut tx).await?;
    let num_writers = writers.len();

    let transport_futs = writers.into_iter().map(write_one_row);

    try_join(import_fut.map_err(From::from), try_join_all(transport_futs)).await?;

    tx.commit().await?;

    let num_rows: i64 = sqlx_exasol::query_scalar("SELECT COUNT(*) FROM TEST_ETL")
        .fetch_one(&mut *conn)
        .await?;

    assert_eq!(num_rows, (NUM_ROWS + num_writers) as i64);

    Ok(())
}

// // Not much to do about this... This is commented out as the IMPORT
// // is limited by Exasol in the sense that spawned writer MUST be used.
// //
// // This will thus fail, because Exasol will just keep sending new requests.
// #[ignore]
// #[sqlx_exasol::test]
// async fn test_etl_close_writer(mut conn: PoolConnection<Exasol>) -> Result<(), BoxDynError> {
//
//     async fn pipe_close_writers(mut writer: ExaImport) -> Result<(), BoxDynError> {
//         writer.close().await?;
//         Ok(())
//     }
//
//     rustls::crypto::CryptoProvider::install_default(rustls::crypto::aws_lc_rs::default_provider())
//        .ok();
//
//     conn.execute("CREATE TABLE TEST_ETL ( col VARCHAR(200) );")
//         .await?;

//     let (import_fut, writers) = ImportBuilder::new("TEST_ETL").build(&mut *conn).await?;
//     let num_writers = writers.len();

//     let transport_futs = writers.into_iter().map(pipe_close_writers);

//     try_join(import_fut.map_err(From::from), try_join_all(transport_futs))
//         .await
//         .map_err(|e| anyhow::anyhow!("{e}"))?;

//     let num_rows: u64 = sqlx_exasol::query_scalar("SELECT COUNT(*) FROM TEST_ETL")
//         .fetch_one(&mut *conn)
//         .await?;

//     assert_eq!(num_rows, (NUM_ROWS + num_writers) as u64);

//     Ok(())
// }

// ##########################################
// ############### Utilities ################
// ##########################################

async fn read_data(mut reader: ExaExport) -> Result<(), BoxDynError> {
    let mut buf = String::new();
    reader.read_to_string(&mut buf).await?;
    Ok(())
}

async fn write_one_row(mut writer: ExaImport) -> Result<(), BoxDynError> {
    writer.write_all(b"blabla\r\n").await?;
    writer.close().await?;
    Ok(())
}

async fn drop_some_readers(idx: usize, mut reader: ExaExport) -> Result<(), BoxDynError> {
    if idx % 2 == 0 {
        let _ = reader.read(&mut [0; 1000]).await?;
        return Ok(());
    }

    let mut buf = String::new();
    reader.read_to_string(&mut buf).await?;
    Ok(())
}

async fn pipe_flush_writers(
    mut reader: ExaExport,
    mut writer: ExaImport,
) -> Result<(), BoxDynError> {
    // test if flushing is fine even before any write.
    writer.flush().await?;

    let mut buf = String::new();
    reader.read_to_string(&mut buf).await?;

    writer.write_all(buf.as_bytes()).await?;
    writer.close().await?;

    Ok(())
}

async fn pipe(mut reader: ExaExport, mut writer: ExaImport) -> Result<(), BoxDynError> {
    let mut buf = vec![0; 5120].into_boxed_slice();
    let mut read = 1;

    while read > 0 {
        read = reader.read(&mut buf).await?;
        writer.write_all(&buf[..read]).await?;
    }

    writer.close().await?;
    Ok(())
}
