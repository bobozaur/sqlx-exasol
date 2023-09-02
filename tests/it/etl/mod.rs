use anyhow::Result as AnyResult;

use futures_util::{
    future::{try_join, try_join3, try_join_all},
    AsyncReadExt, AsyncWriteExt, TryFutureExt,
};
use sqlx::Executor;
use sqlx_core::{
    error::BoxDynError,
    pool::{PoolConnection, PoolOptions},
};
use sqlx_exasol::{
    etl::{ExaExport, ExaImport, ExportBuilder, ImportBuilder, QueryOrTable},
    ExaConnectOptions, Exasol,
};
use std::iter;

const NUM_ROWS: usize = 1_000_000;

use macros::{test_etl_multi_threaded, test_etl_single_threaded};

use self::macros::test_etl;

test_etl_single_threaded!(
    "uncompressed",
    "TEST_ETL",
    ExportBuilder::new(QueryOrTable::Table("TEST_ETL")),
    ImportBuilder::new("TEST_ETL"),
);

test_etl_single_threaded!(
    "uncompressed_with_feature",
    "TEST_ETL",
    ExportBuilder::new(QueryOrTable::Table("TEST_ETL")).compression(false),
    ImportBuilder::new("TEST_ETL").compression(false),
    #[cfg(feature = "compression")]
);

test_etl_multi_threaded!(
    "uncompressed",
    "TEST_ETL",
    ExportBuilder::new(QueryOrTable::Table("TEST_ETL")),
    ImportBuilder::new("TEST_ETL"),
);

test_etl_multi_threaded!(
    "uncompressed_with_feature",
    "TEST_ETL",
    ExportBuilder::new(QueryOrTable::Table("TEST_ETL")).compression(false),
    ImportBuilder::new("TEST_ETL").compression(false),
    #[cfg(feature = "compression")]
);

test_etl_single_threaded!(
    "compressed",
    "TEST_ETL",
    ExportBuilder::new(QueryOrTable::Table("TEST_ETL")).compression(true),
    ImportBuilder::new("TEST_ETL").compression(true),
    #[cfg(feature = "compression")]
);

test_etl_multi_threaded!(
    "compressed",
    "TEST_ETL",
    ExportBuilder::new(QueryOrTable::Table("TEST_ETL")).compression(true),
    ImportBuilder::new("TEST_ETL").compression(true),
    #[cfg(feature = "compression")]
);

test_etl_single_threaded!(
    "query_export",
    "TEST_ETL",
    ExportBuilder::new(QueryOrTable::Query("SELECT * FROM TEST_ETL")),
    ImportBuilder::new("TEST_ETL"),
);

test_etl_single_threaded!(
    "multiple_workers",
    3,
    "TEST_ETL",
    ExportBuilder::new(QueryOrTable::Table("TEST_ETL")),
    ImportBuilder::new("TEST_ETL"),
);

test_etl_single_threaded!(
    "multiple_workers_compressed",
    3,
    "TEST_ETL",
    ExportBuilder::new(QueryOrTable::Table("TEST_ETL")).compression(true),
    ImportBuilder::new("TEST_ETL").compression(true),
    #[cfg(feature = "compression")]
);

test_etl_multi_threaded!(
    "multiple_workers",
    3,
    "TEST_ETL",
    ExportBuilder::new(QueryOrTable::Table("TEST_ETL")),
    ImportBuilder::new("TEST_ETL"),
);

test_etl_multi_threaded!(
    "multiple_workers_compressed",
    3,
    "TEST_ETL",
    ExportBuilder::new(QueryOrTable::Table("TEST_ETL")).compression(true),
    ImportBuilder::new("TEST_ETL").compression(true),
    #[cfg(feature = "compression")]
);

test_etl_single_threaded!(
    "all_arguments",
    "TEST_ETL",
    ExportBuilder::new(QueryOrTable::Table("TEST_ETL")).num_readers(1).compression(false).comment("test").encoding("ASCII").null("OH-NO").row_separator(sqlx_exasol::etl::RowSeparator::LF).column_separator("|").column_delimiter("\\\\").with_column_names(true),
    ImportBuilder::new("TEST_ETL").skip(1).buffer_size(20000).columns(Some(&["col"])).num_writers(1).compression(false).comment("test").encoding("ASCII").null("OH-NO").row_separator(sqlx_exasol::etl::RowSeparator::LF).column_separator("|").column_delimiter("\\\\").trim(sqlx_exasol::etl::Trim::Both),
    #[cfg(feature = "compression")]
);

test_etl!(
    "single_threaded",
    "writer_flush_first",
    1,
    "TEST_ETL",
    |(r, w)| pipe_flush_writers(r, w),
    ExportBuilder::new(QueryOrTable::Table("TEST_ETL")),
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

    let (export_fut, readers) = ExportBuilder::new(QueryOrTable::Table(";)BAD_TABLE_NAME*&"))
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

    let (export_fut, readers) = ExportBuilder::new(QueryOrTable::Table("TEST_ETL"))
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
async fn test_etl_writer_close_without_write(mut conn: PoolConnection<Exasol>) -> AnyResult<()> {
    async fn close_writer(mut writer: ExaImport) -> Result<(), BoxDynError> {
        writer.close().await?;
        Ok(())
    }

    conn.execute("CREATE TABLE TEST_ETL ( col VARCHAR(200) );")
        .await?;

    sqlx::query("INSERT INTO TEST_ETL VALUES (?)")
        .bind(vec!["dummy"; NUM_ROWS])
        .execute(&mut *conn)
        .await?;

    let (import_fut, writers) = ImportBuilder::new("TEST_ETL").build(&mut conn).await?;

    let transport_futs = writers.into_iter().map(close_writer);

    try_join(import_fut.map_err(From::from), try_join_all(transport_futs))
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))
        .unwrap_err();

    Ok(())
}

// ##########################################
// ############### Utilities ################
// ##########################################
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

async fn pipe(mut reader: ExaExport, mut writer: ExaImport) -> AnyResult<()> {
    let mut buf = String::new();
    reader.read_to_string(&mut buf).await?;
    writer.write_all(buf.as_bytes()).await?;
    writer.close().await?;
    Ok(())
}

mod macros {
    macro_rules! test_etl {
    ($kind:literal, $name:literal, $num_workers:expr, $table:literal, $proc:expr, $export:expr, $import:expr, $(#[$attr:meta]),*) => {
        paste::item! {
            $(#[$attr]),*
            #[ignore]
            #[sqlx::test]
            async fn [< test_etl_ $kind _ $name >](pool_opts: PoolOptions<Exasol>, exa_opts: ExaConnectOptions) -> AnyResult<()> {
                let pool = pool_opts.min_connections(2).connect_with(exa_opts).await?;

                let mut conn1 = pool.acquire().await?;
                let mut conn2 = pool.acquire().await?;

                conn1
                    .execute(concat!("CREATE TABLE ", $table, " ( col VARCHAR(200) );"))
                    .await?;

                sqlx::query(concat!("INSERT INTO ", $table, " VALUES (?)"))
                    .bind(vec!["dummy"; NUM_ROWS])
                    .execute(&mut *conn1)
                    .await?;

                let (export_fut, readers) = $export.num_readers($num_workers).build(&mut conn1).await?;

                let (import_fut, writers) = $import.num_writers($num_workers).build(&mut conn2).await?;

                assert_eq!(readers.len(), $num_workers);
                assert_eq!(writers.len(), $num_workers);

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
}

    macro_rules! test_etl_single_threaded {
        ($name:literal, $table:literal, $export:expr, $import:expr, $(#[$attr:meta]),*) => {
            $crate::etl::macros::test_etl_single_threaded!($name, 1, $table, $export, $import, $(#[$attr]),*);
        };

        ($name:literal, $num_workers:expr, $table:literal, $export:expr, $import:expr, $(#[$attr:meta]),*) => {
            $crate::etl::macros::test_etl!("single_threaded", $name, $num_workers, $table, |(r,w)|  pipe(r, w), $export, $import, $(#[$attr]),*);
        }
    }

    macro_rules! test_etl_multi_threaded {
        ($name:literal, $table:literal, $export:expr, $import:expr, $(#[$attr:meta]),*) => {
            $crate::etl::macros::test_etl_multi_threaded!($name, 1, $table, $export, $import, $(#[$attr]),*);
        };

        ($name:literal, $num_workers:expr, $table:literal, $export:expr, $import:expr, $(#[$attr:meta]),*) => {
            $crate::etl::macros::test_etl!("multi_threaded", $name, $num_workers, $table, |(r,w)|  tokio::spawn(pipe(r, w)).map_err(From::from).and_then(|r| async { r }), $export, $import, $(#[$attr]),*);
        }
    }

    pub(crate) use test_etl;
    pub(crate) use test_etl_multi_threaded;
    pub(crate) use test_etl_single_threaded;
}
