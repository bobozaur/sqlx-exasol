use exasol::etl::{ExaExport, ExaImport, ExportBuilder, ImportBuilder, QueryOrTable};
use futures_util::{
    future::{try_join3, try_join_all},
    AsyncReadExt, AsyncWriteExt, TryFutureExt,
};
use sqlx::Executor;
use sqlx_core::error::BoxDynError;
use std::iter;

const NUM_ROWS: usize = 1_000_000;

use macros::test_threaded_etl;

test_threaded_etl!(
    single:
    "uncompressed",
    "TEST_ETL",
    ExportBuilder::new(QueryOrTable::Table("TEST_ETL")),
    ImportBuilder::new("TEST_ETL").skip(1),
);

test_threaded_etl!(
    single:
    "uncompressed_with_feature",
    "TEST_ETL",
    ExportBuilder::new(QueryOrTable::Table("TEST_ETL")).compression(false),
    ImportBuilder::new("TEST_ETL").skip(1).compression(false),
    #[cfg(feature = "compression")]
);

test_threaded_etl!(
    multi:
    "uncompressed",
    "TEST_ETL",
    ExportBuilder::new(QueryOrTable::Table("TEST_ETL")),
    ImportBuilder::new("TEST_ETL").skip(1),
);

test_threaded_etl!(
    multi:
    "uncompressed_with_feature",
    "TEST_ETL",
    ExportBuilder::new(QueryOrTable::Table("TEST_ETL")).compression(false),
    ImportBuilder::new("TEST_ETL").skip(1).compression(false),
    #[cfg(feature = "compression")]
);

test_threaded_etl!(
    single:
    "compressed",
    "TEST_ETL",
    ExportBuilder::new(QueryOrTable::Table("TEST_ETL")).compression(true),
    ImportBuilder::new("TEST_ETL").skip(1).compression(true),
    #[cfg(feature = "compression")]
);

test_threaded_etl!(
    multi:
    "compressed",
    "TEST_ETL",
    ExportBuilder::new(QueryOrTable::Table("TEST_ETL")).compression(true),
    ImportBuilder::new("TEST_ETL").skip(1).compression(true),
    #[cfg(feature = "compression")]
);

test_threaded_etl!(
    single:
    "query_export",
    "TEST_ETL",
    ExportBuilder::new(QueryOrTable::Query("SELECT * FROM TEST_ETL")),
    ImportBuilder::new("TEST_ETL").skip(1),
);

test_threaded_etl!(
    single:
    "limit_workers",
    "TEST_ETL",
    ExportBuilder::new(QueryOrTable::Table("TEST_ETL")).num_readers(1),
    ImportBuilder::new("TEST_ETL").skip(1).num_writers(1),
);

test_threaded_etl!(
    single:
    "all_arguments",
    "TEST_ETL",
    ExportBuilder::new(QueryOrTable::Table("TEST_ETL")).num_readers(1).compression(false).comment("test").encoding("ASCII").null("OH-NO").row_separator(exasol::etl::RowSeparator::LF).column_separator("|").column_delimiter("\\\\").with_column_names(true),
    ImportBuilder::new("TEST_ETL").skip(1).buffer_size(20000).columns(Some(&["col"])).num_writers(1).compression(false).comment("test").encoding("ASCII").null("OH-NO").row_separator(exasol::etl::RowSeparator::LF).column_separator("|").column_delimiter("\\\\").trim(exasol::etl::Trim::Both),
    #[cfg(feature = "compression")]
);

#[should_panic]
#[sqlx::test]
async fn test_etl_invalid_query(pool: sqlx::Pool<exasol::Exasol>) {
    let mut conn1 = pool.acquire().await.unwrap();

    conn1
        .execute("CREATE TABLE TEST_ETL ( col VARCHAR(200) );")
        .await
        .unwrap();

    sqlx::query("INSERT INTO TEST_ETL VALUES (?)")
        .bind(vec!["dummy"; NUM_ROWS])
        .execute(&mut *conn1)
        .await
        .unwrap();

    ExportBuilder::new(QueryOrTable::Table(";)BAD_TABLE_NAME*&"))
        .build(&mut conn1)
        .await
        .unwrap();
}

#[should_panic]
#[sqlx::test]
async fn test_etl_reader_drop(pool: sqlx::Pool<exasol::Exasol>) {
    async fn drop_some_readers(
        idx: usize,
        mut reader: ExaExport,
        mut writer: ExaImport,
    ) -> Result<(), BoxDynError> {
        if idx % 2 == 0 {
            let buf = "val1\r\nval2\r\nval3\r\n";

            let _ = reader.read(&mut [0; 1000]).await?;

            writer.write_all(buf.as_bytes()).await?;
            writer.close().await?;

            return Ok(());
        }

        let mut buf = String::new();
        reader.read_to_string(&mut buf).await?;

        writer.write_all(buf.as_bytes()).await?;
        writer.close().await?;

        Ok(())
    }

    let mut conn1 = pool.acquire().await.unwrap();
    let mut conn2 = pool.acquire().await.unwrap();

    conn1
        .execute("CREATE TABLE TEST_ETL ( col VARCHAR(200) );")
        .await
        .unwrap();

    sqlx::query("INSERT INTO TEST_ETL VALUES (?)")
        .bind(vec!["dummy"; NUM_ROWS])
        .execute(&mut *conn1)
        .await
        .unwrap();

    let (export_fut, readers) = (ExportBuilder::new(QueryOrTable::Table("TEST_ETL")))
        .build(&mut conn1)
        .await
        .unwrap();

    let (import_fut, writers) = (ImportBuilder::new("TEST_ETL").skip(1))
        .build(&mut conn2)
        .await
        .unwrap();

    let transport_futs = iter::zip(readers, writers)
        .enumerate()
        .map(|(idx, (r, w))| drop_some_readers(idx, r, w));

    try_join3(
        export_fut.map_err(From::from),
        import_fut.map_err(From::from),
        try_join_all(transport_futs),
    )
    .await
    .unwrap();
}

#[sqlx::test]
async fn test_etl_writer_flush_first(pool: sqlx::Pool<exasol::Exasol>) -> anyhow::Result<()> {
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

    let mut conn1 = pool.acquire().await?;
    let mut conn2 = pool.acquire().await?;

    conn1
        .execute("CREATE TABLE TEST_ETL ( col VARCHAR(200) );")
        .await?;

    sqlx::query("INSERT INTO TEST_ETL VALUES (?)")
        .bind(vec!["dummy"; NUM_ROWS])
        .execute(&mut *conn1)
        .await?;

    let (export_fut, readers) = (ExportBuilder::new(QueryOrTable::Table("TEST_ETL")))
        .build(&mut conn1)
        .await?;

    let (import_fut, writers) = (ImportBuilder::new("TEST_ETL").skip(1))
        .build(&mut conn2)
        .await?;

    let transport_futs = iter::zip(readers, writers).map(|(r, w)| pipe_flush_writers(r, w));

    try_join3(
        export_fut.map_err(From::from),
        import_fut.map_err(From::from),
        try_join_all(transport_futs),
    )
    .await
    .map_err(|e| anyhow::anyhow!("{e}"))?;

    Ok(())
}

#[sqlx::test]
async fn test_etl_writer_close_without_write(
    pool: sqlx::Pool<exasol::Exasol>,
) -> anyhow::Result<()> {
    async fn pipe_flush_writers(
        mut reader: ExaExport,
        mut writer: ExaImport,
    ) -> Result<(), BoxDynError> {
        writer.close().await?;

        let mut buf = String::new();
        reader.read_to_string(&mut buf).await?;

        Ok(())
    }

    let mut conn1 = pool.acquire().await?;
    let mut conn2 = pool.acquire().await?;

    conn1
        .execute("CREATE TABLE TEST_ETL ( col VARCHAR(200) );")
        .await?;

    sqlx::query("INSERT INTO TEST_ETL VALUES (?)")
        .bind(vec!["dummy"; NUM_ROWS])
        .execute(&mut *conn1)
        .await?;

    let (export_fut, readers) = (ExportBuilder::new(QueryOrTable::Table("TEST_ETL")))
        .build(&mut conn1)
        .await?;

    let (import_fut, writers) = (ImportBuilder::new("TEST_ETL").skip(1))
        .build(&mut conn2)
        .await?;

    let transport_futs = iter::zip(readers, writers).map(|(r, w)| pipe_flush_writers(r, w));

    try_join3(
        export_fut.map_err(From::from),
        import_fut.map_err(From::from),
        try_join_all(transport_futs),
    )
    .await
    .map_err(|e| anyhow::anyhow!("{e}"))?;

    Ok(())
}

mod macros {
    macro_rules! test_threaded_etl {
    ($type:literal, $name:literal, $table:literal, $proc:expr, $export:expr, $import:expr, $(#[$attr:meta]),*) => {
        paste::item! {
            $(#[$attr]),*
            #[sqlx::test]
            async fn [< test_etl_ $type _ $name >](
                pool: sqlx::Pool<exasol::Exasol>,
            ) -> anyhow::Result<()> {
                async fn pipe(mut reader: ExaExport, mut writer: ExaImport) -> Result<(), BoxDynError> {
                    let mut buf = String::new();
                    reader.read_to_string(&mut buf).await?;
                    writer.write_all(buf.as_bytes()).await?;
                    writer.close().await?;
                    Ok(())
                }


                let mut conn1 = pool.acquire().await?;
                let mut conn2 = pool.acquire().await?;

                conn1
                    .execute(concat!("CREATE TABLE ", $table, " ( col VARCHAR(200) );"))
                    .await?;

                sqlx::query(concat!("INSERT INTO ", $table, " VALUES (?)"))
                    .bind(vec!["dummy"; NUM_ROWS])
                    .execute(&mut *conn1)
                    .await?;

                let (export_fut, readers) = $export.build(&mut conn1).await?;

                let (import_fut, writers) = $import.build(&mut conn2).await?;

                let transport_futs = iter::zip(readers, writers).map($proc);

                let (export_res, import_res, _) =
                try_join3(export_fut.map_err(From::from), import_fut.map_err(From::from), try_join_all(transport_futs)).await.map_err(|e| anyhow::anyhow! {e})?;


                assert_eq!(NUM_ROWS as u64, export_res.rows_affected());
                assert_eq!(NUM_ROWS as u64, import_res.rows_affected());

                let num_rows: u64 = sqlx::query_scalar(concat!("SELECT COUNT(*) FROM ", $table))
                    .fetch_one(&mut *conn1)
                    .await?;

                assert_eq!(num_rows, 2 * NUM_ROWS as u64);

                Ok(())
            }
        }
    };

    (single: $name:literal, $table:literal, $export:expr, $import:expr, $(#[$attr:meta]),*) => {
        test_threaded_etl!("single_threaded", $name, $table, |(r,w)|  pipe(r, w), $export, $import, $(#[$attr]),*);
    };

    (multi: $name:literal, $table:literal, $export:expr, $import:expr, $(#[$attr:meta]),*) => {
        test_threaded_etl!("multi_threaded", $name, $table, |(r,w)|  tokio::spawn(pipe(r, w)).map_err(From::from).and_then(|r| async { r }), $export, $import, $(#[$attr]),*);
    }
}

    pub(crate) use test_threaded_etl;
}
