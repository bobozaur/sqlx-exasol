#![cfg(feature = "migrate")]
#![cfg(feature = "etl")]

mod macros;

use std::iter;

use anyhow::Result as AnyResult;
use futures_util::{
    future::{try_join, try_join3, try_join_all},
    AsyncReadExt, AsyncWriteExt, TryFutureExt,
};
use sqlx::{Connection, Executor};
use sqlx_core::{
    error::BoxDynError,
    pool::{PoolConnection, PoolOptions},
};
use sqlx_exasol::{
    etl::{ExaExport, ExaImport, ExportBuilder, ExportSource, ImportBuilder},
    ExaConnectOptions, Exasol,
};

const NUM_ROWS: usize = 1_000_000;

test_etl_single_threaded!(
    "uncompressed",
    "TEST_ETL",
    ExportBuilder::new(ExportSource::Table("TEST_ETL")),
    ImportBuilder::new("TEST_ETL"),
);

test_etl_single_threaded!(
    "uncompressed_with_feature",
    "TEST_ETL",
    ExportBuilder::new(ExportSource::Table("TEST_ETL")).compression(false),
    ImportBuilder::new("TEST_ETL").compression(false),
    #[cfg(feature = "compression")]
);

test_etl_multi_threaded!(
    "uncompressed",
    "TEST_ETL",
    ExportBuilder::new(ExportSource::Table("TEST_ETL")),
    ImportBuilder::new("TEST_ETL"),
);

test_etl_multi_threaded!(
    "uncompressed_with_feature",
    "TEST_ETL",
    ExportBuilder::new(ExportSource::Table("TEST_ETL")).compression(false),
    ImportBuilder::new("TEST_ETL").compression(false),
    #[cfg(feature = "compression")]
);

test_etl_single_threaded!(
    "compressed",
    "TEST_ETL",
    ExportBuilder::new(ExportSource::Table("TEST_ETL")).compression(true),
    ImportBuilder::new("TEST_ETL").compression(true),
    #[cfg(feature = "compression")]
);

test_etl_multi_threaded!(
    "compressed",
    "TEST_ETL",
    ExportBuilder::new(ExportSource::Table("TEST_ETL")).compression(true),
    ImportBuilder::new("TEST_ETL").compression(true),
    #[cfg(feature = "compression")]
);

test_etl_single_threaded!(
    "query_export",
    "TEST_ETL",
    ExportBuilder::new(ExportSource::Query("SELECT * FROM TEST_ETL")),
    ImportBuilder::new("TEST_ETL"),
);

test_etl_single_threaded!(
    "multiple_workers",
    0,
    "TEST_ETL",
    ExportBuilder::new(ExportSource::Table("TEST_ETL")),
    ImportBuilder::new("TEST_ETL"),
);

test_etl_single_threaded!(
    "multiple_workers_compressed",
    0,
    "TEST_ETL",
    ExportBuilder::new(ExportSource::Table("TEST_ETL")).compression(true),
    ImportBuilder::new("TEST_ETL").compression(true),
    #[cfg(feature = "compression")]
);

test_etl_multi_threaded!(
    "multiple_workers",
    0,
    "TEST_ETL",
    ExportBuilder::new(ExportSource::Table("TEST_ETL")),
    ImportBuilder::new("TEST_ETL"),
);

test_etl_multi_threaded!(
    "multiple_workers_compressed",
    0,
    "TEST_ETL",
    ExportBuilder::new(ExportSource::Table("TEST_ETL")).compression(true),
    ImportBuilder::new("TEST_ETL").compression(true),
    #[cfg(feature = "compression")]
);

test_etl_single_threaded!(
    "all_arguments",
    "TEST_ETL",
    ExportBuilder::new(ExportSource::Table("TEST_ETL")).num_readers(1).compression(false).comment("test").encoding("ASCII").null("OH-NO").row_separator(sqlx_exasol::etl::RowSeparator::LF).column_separator("|").column_delimiter("\\\\").with_column_names(true),
    ImportBuilder::new("TEST_ETL").skip(1).buffer_size(20000).columns(Some(&["col"])).num_writers(1).compression(false).comment("test").encoding("ASCII").null("OH-NO").row_separator(sqlx_exasol::etl::RowSeparator::LF).column_separator("|").column_delimiter("\\\\").trim(sqlx_exasol::etl::Trim::Both),
    #[cfg(feature = "compression")]
);

test_etl!(
    "single_threaded",
    "writer_flush_first",
    1,
    "TEST_ETL",
    |(r, w)| pipe_flush_writers(r, w),
    ExportBuilder::new(ExportSource::Table("TEST_ETL")),
    ImportBuilder::new("TEST_ETL"),
);

// ##########################################
// ################ Failures ################
// ##########################################
#[ignore]
#[sqlx::test]
async fn test_etl_invalid_query(mut conn: PoolConnection<Exasol>) -> AnyResult<()> {
    async fn read_data(mut reader: ExaExport) -> AnyResult<()> {
        let mut buf = String::new();
        reader.read_to_string(&mut buf).await?;
        Ok(())
    }

    conn.execute("CREATE TABLE TEST_ETL ( col VARCHAR(200) );")
        .await?;

    sqlx::query("INSERT INTO TEST_ETL VALUES (?)")
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
#[sqlx::test]
async fn test_etl_reader_drop(mut conn: PoolConnection<Exasol>) -> AnyResult<()> {
    conn.execute("CREATE TABLE TEST_ETL ( col VARCHAR(200) );")
        .await?;

    sqlx::query("INSERT INTO TEST_ETL VALUES (?)")
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
#[sqlx::test]
async fn test_etl_transaction_import_rollback(mut conn: PoolConnection<Exasol>) -> AnyResult<()> {
    conn.execute("CREATE TABLE TEST_ETL ( col VARCHAR(200) );")
        .await?;

    sqlx::query("INSERT INTO TEST_ETL VALUES (?)")
        .bind(vec!["dummy"; NUM_ROWS])
        .execute(&mut *conn)
        .await?;

    let mut tx = conn.begin().await?;

    let (import_fut, writers) = ImportBuilder::new("TEST_ETL").build(&mut tx).await?;

    let transport_futs = writers.into_iter().map(write_one_row);

    try_join(import_fut.map_err(From::from), try_join_all(transport_futs))
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    tx.rollback().await?;

    let num_rows: u64 = sqlx::query_scalar("SELECT COUNT(*) FROM TEST_ETL")
        .fetch_one(&mut *conn)
        .await?;

    assert_eq!(num_rows, NUM_ROWS as u64);

    Ok(())
}

#[ignore]
#[sqlx::test]
async fn test_etl_transaction_import_commit(mut conn: PoolConnection<Exasol>) -> AnyResult<()> {
    conn.execute("CREATE TABLE TEST_ETL ( col VARCHAR(200) );")
        .await?;

    sqlx::query("INSERT INTO TEST_ETL VALUES (?)")
        .bind(vec!["dummy"; NUM_ROWS])
        .execute(&mut *conn)
        .await?;

    let mut tx = conn.begin().await?;

    let (import_fut, writers) = ImportBuilder::new("TEST_ETL").build(&mut tx).await?;
    let num_writers = writers.len();

    let transport_futs = writers.into_iter().map(write_one_row);

    try_join(import_fut.map_err(From::from), try_join_all(transport_futs))
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    tx.commit().await?;

    let num_rows: u64 = sqlx::query_scalar("SELECT COUNT(*) FROM TEST_ETL")
        .fetch_one(&mut *conn)
        .await?;

    assert_eq!(num_rows, (NUM_ROWS + num_writers) as u64);

    Ok(())
}

// // Not much to do about this... This is commented out as the IMPORT
// // is limited by Exasol in the sense that spawned writer MUST be used.
// //
// // This will thus fail, because Exasol will just keep sending new requests.
// #[ignore]
// #[sqlx::test]
// async fn test_etl_close_writer(mut conn: PoolConnection<Exasol>) -> AnyResult<()> {
//     conn.execute("CREATE TABLE TEST_ETL ( col VARCHAR(200) );")
//         .await?;

//     let (import_fut, writers) = ImportBuilder::new("TEST_ETL").build(&mut *conn).await?;
//     let num_writers = writers.len();

//     let transport_futs = writers.into_iter().map(pipe_close_writers);

//     try_join(import_fut.map_err(From::from), try_join_all(transport_futs))
//         .await
//         .map_err(|e| anyhow::anyhow!("{e}"))?;

//     let num_rows: u64 = sqlx::query_scalar("SELECT COUNT(*) FROM TEST_ETL")
//         .fetch_one(&mut *conn)
//         .await?;

//     assert_eq!(num_rows, (NUM_ROWS + num_writers) as u64);

//     Ok(())
// }

// ##########################################
// ############### Utilities ################
// ##########################################

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

async fn pipe_flush_writers(mut reader: ExaExport, mut writer: ExaImport) -> AnyResult<()> {
    // test if flushing is fine even before any write.
    writer.flush().await?;

    let mut buf = String::new();
    reader.read_to_string(&mut buf).await?;

    writer.write_all(buf.as_bytes()).await?;
    writer.close().await?;

    Ok(())
}

// async fn pipe_close_writers(mut writer: ExaImport) -> AnyResult<()> {
//     writer.close().await?;
//     Ok(())
// }

async fn pipe(mut reader: ExaExport, mut writer: ExaImport) -> AnyResult<()> {
    let mut buf = vec![0; 5120].into_boxed_slice();
    let mut read = 1;

    while read > 0 {
        read = reader.read(&mut buf).await?;
        writer.write_all(&buf[..read]).await?;
    }

    writer.close().await?;
    Ok(())
}
