#![cfg(all(feature = "migrate", feature = "etl"))]

mod macros;

use std::{iter, time::Duration};

use futures_util::{
    future::{try_join, try_join3, try_join_all},
    AsyncReadExt, AsyncWriteExt, TryFutureExt,
};
use sqlx_exasol::{
    error::BoxDynError,
    etl::{ExaExport, ExaImport, ExportBuilder, ImportBuilder},
    pool::{PoolConnection, PoolOptions},
    Connection, ExaConnectOptions, Exasol, Executor,
};

const NUM_ROWS: usize = 500_000;

test_etl_single_threaded!(
    "simple",
    "TEST_ETL",
    ExportBuilder::new_from_table("TEST_ETL", None),
    ImportBuilder::new("TEST_ETL")
);

test_etl_multi_threaded!(
    "simple",
    "TEST_ETL",
    ExportBuilder::new_from_table("TEST_ETL", None),
    ImportBuilder::new("TEST_ETL")
);

test_etl_single_threaded!(
    "query_export",
    "TEST_ETL",
    ExportBuilder::new_from_query("SELECT * FROM TEST_ETL"),
    ImportBuilder::new("TEST_ETL")
);

test_etl_single_threaded!(
    "multiple_workers",
    0,
    "TEST_ETL",
    ExportBuilder::new_from_table("TEST_ETL", None),
    ImportBuilder::new("TEST_ETL")
);

test_etl_multi_threaded!(
    "multiple_workers",
    0,
    "TEST_ETL",
    ExportBuilder::new_from_table("TEST_ETL", None),
    ImportBuilder::new("TEST_ETL")
);

test_etl_single_threaded!(
    "all_arguments",
    "TEST_ETL",
    ExportBuilder::new_from_table("TEST_ETL", None)
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
        .columns(Some(&["col", "num", "empty"]))
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
    ExportBuilder::new_from_table("TEST_ETL", None),
    ImportBuilder::new("TEST_ETL")
);

// ##########################################
// ################ Failures ################
// ##########################################
#[ignore]
#[sqlx_exasol::test]
async fn test_etl_invalid_query(mut conn: PoolConnection<Exasol>) -> Result<(), BoxDynError> {
    async fn read_data(mut reader: ExaExport) -> Result<(), BoxDynError> {
        let mut buf = String::new();
        reader.read_to_string(&mut buf).await?;
        Ok(())
    }

    conn.execute("CREATE TABLE TEST_ETL ( col VARCHAR(200) );")
        .await?;

    sqlx_exasol::query("INSERT INTO TEST_ETL VALUES (?)")
        .bind(vec!["dummy"; NUM_ROWS])
        .execute(&mut *conn)
        .await?;

    let (export_fut, readers) = ExportBuilder::new_from_table(";)BAD_TABLE_NAME*&", None)
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
    async fn drop_some_readers(idx: usize, mut reader: ExaExport) -> Result<(), BoxDynError> {
        if idx % 2 == 0 {
            let _ = reader.read(&mut [0; 100]).await?;
            return Ok(());
        }

        let mut buf = String::new();
        reader.read_to_string(&mut buf).await?;
        Ok(())
    }

    // Using multiple columns because if there's too little data the reader might just buffer it all
    // before being dropped and that will cause the background server to properly respond
    // to Exasol's HTTP request.
    conn.execute(
        r#"
        CREATE TABLE TEST_ETL(
            col VARCHAR(200),
            col2 VARCHAR(200),
            col3 VARCHAR(200),
            col4 VARCHAR(200),
            col5 VARCHAR(200),
            col6 VARCHAR(200)
        );"#,
    )
    .await?;

    sqlx_exasol::query("INSERT INTO TEST_ETL VALUES (?, ?, ?, ?, ?, ?)")
        .bind(vec!["dummy"; NUM_ROWS])
        .bind(vec!["dummy"; NUM_ROWS])
        .bind(vec!["dummy"; NUM_ROWS])
        .bind(vec!["dummy"; NUM_ROWS])
        .bind(vec!["dummy"; NUM_ROWS])
        .bind(vec!["dummy"; NUM_ROWS])
        .execute(&mut *conn)
        .await?;

    let (export_fut, readers) = ExportBuilder::new_from_table("TEST_ETL", None)
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

#[ignore]
#[sqlx_exasol::test]
async fn test_etl_close_all_but_one_writers(
    mut conn: PoolConnection<Exasol>,
) -> Result<(), BoxDynError> {
    async fn pipe_close_writers(
        idx: usize,
        winner: usize,
        mut writer: ExaImport,
    ) -> Result<(), BoxDynError> {
        if idx == winner {
            writer.write_all(b"blabla\r\n").await?;
            writer.close().await?;
        } else {
            sqlx_exasol::__rt::sleep(Duration::from_millis(1000)).await;
            writer.close().await?;
        }
        Ok(())
    }

    conn.execute("CREATE TABLE TEST_ETL ( col VARCHAR(200) );")
        .await?;

    let (import_fut, writers) = ImportBuilder::new("TEST_ETL").build(&mut conn).await?;
    let winner = rand::random::<usize>() % writers.len();

    let transport_futs = writers
        .into_iter()
        .enumerate()
        .map(|(idx, writer)| pipe_close_writers(idx, winner, writer));

    try_join(import_fut.map_err(From::from), try_join_all(transport_futs)).await?;

    let num_rows: i64 = sqlx_exasol::query_scalar("SELECT COUNT(*) FROM TEST_ETL")
        .fetch_one(&mut *conn)
        .await?;

    assert_eq!(num_rows, 1);

    Ok(())
}

#[ignore]
#[sqlx_exasol::test]
async fn test_etl_drop_reader_without_deadlock(
    mut conn: PoolConnection<Exasol>,
) -> Result<(), BoxDynError> {
    async fn drop_reader(idx: usize, mut reader: ExaExport) -> Result<(), BoxDynError> {
        if idx == 0 {
            return Ok(());
        }

        let mut buf = String::new();
        reader.read_to_string(&mut buf).await?;
        Ok(())
    }

    conn.execute("CREATE TABLE TEST_ETL(col VARCHAR(200));")
        .await?;

    sqlx_exasol::query("INSERT INTO TEST_ETL VALUES (?)")
        .bind(vec!["dummy"; NUM_ROWS])
        .execute(&mut *conn)
        .await?;

    let (export_fut, readers) = ExportBuilder::new_from_table("TEST_ETL", None)
        .build(&mut conn)
        .await?;

    let transport_futs = readers
        .into_iter()
        .enumerate()
        .map(|(idx, r)| drop_reader(idx, r));

    try_join(export_fut.map_err(From::from), try_join_all(transport_futs))
        .await
        .unwrap_err();

    Ok(())
}

#[ignore]
#[sqlx_exasol::test]
async fn test_etl_drop_writer_without_deadlock(
    mut conn: PoolConnection<Exasol>,
) -> Result<(), BoxDynError> {
    async fn drop_writer(idx: usize, mut writer: ExaImport) -> Result<(), BoxDynError> {
        if idx == 0 {
            return Ok(());
        }

        writer.write_all(b"blabla\r\n").await?;
        writer.close().await?;

        Ok(())
    }

    conn.execute("CREATE TABLE TEST_ETL ( col VARCHAR(200) );")
        .await?;

    let (import_fut, writers) = ImportBuilder::new("TEST_ETL").build(&mut conn).await?;

    let transport_futs = writers
        .into_iter()
        .enumerate()
        .map(|(idx, writer)| drop_writer(idx, writer));

    try_join(import_fut.map_err(From::from), try_join_all(transport_futs))
        .await
        .unwrap_err();

    Ok(())
}

// ##########################################
// ############### Utilities ################
// ##########################################

async fn write_one_row(mut writer: ExaImport) -> Result<(), BoxDynError> {
    writer.write_all(b"blabla\r\n").await?;
    writer.close().await?;
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
