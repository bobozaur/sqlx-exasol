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

use macros::test_threaded_etl;

const NUM_ROWS: usize = 1_000_000;

test_threaded_etl!(
    single:
    "uncompressed",
    ExportBuilder::new(QueryOrTable::Table("TEST_HTTP_TRANSPORT")),
    ImportBuilder::new("TEST_HTTP_TRANSPORT").skip(1),
);

test_threaded_etl!(
    single:
    "uncompressed_with_feature",
    ExportBuilder::new(QueryOrTable::Table("TEST_HTTP_TRANSPORT")).compression(false),
    ImportBuilder::new("TEST_HTTP_TRANSPORT").skip(1).compression(false),
    #[cfg(feature = "compression")]
);

test_threaded_etl!(
    multi:
    "uncompressed",
    ExportBuilder::new(QueryOrTable::Table("TEST_HTTP_TRANSPORT")),
    ImportBuilder::new("TEST_HTTP_TRANSPORT").skip(1),
);

test_threaded_etl!(
    multi:
    "uncompressed_with_feature",
    ExportBuilder::new(QueryOrTable::Table("TEST_HTTP_TRANSPORT")).compression(false),
    ImportBuilder::new("TEST_HTTP_TRANSPORT").skip(1).compression(false),
    #[cfg(feature = "compression")]
);

test_threaded_etl!(
    single:
    "compressed",
    ExportBuilder::new(QueryOrTable::Table("TEST_HTTP_TRANSPORT")).compression(true),
    ImportBuilder::new("TEST_HTTP_TRANSPORT").skip(1).compression(true),
    #[cfg(feature = "compression")]
);

test_threaded_etl!(
    multi:
    "compressed",
    ExportBuilder::new(QueryOrTable::Table("TEST_HTTP_TRANSPORT")).compression(true),
    ImportBuilder::new("TEST_HTTP_TRANSPORT").skip(1).compression(true),
    #[cfg(feature = "compression")]
);

mod macros {

    macro_rules! test_threaded_etl {
    ($type:literal, $name:literal, $proc:expr, $export:expr, $import:expr, $(#[$attr:meta]),*) => {
        paste::item! {
            $(#[$attr]),*
            #[sqlx::test]
            async fn [< test_etl_ $type _ $name >](
                pool: Pool<Exasol>,
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
                    .execute("CREATE TABLE TEST_HTTP_TRANSPORT ( col VARCHAR(200) );")
                    .await?;

                sqlx::query("INSERT INTO TEST_HTTP_TRANSPORT VALUES (?)")
                    .bind(vec!["dummy"; NUM_ROWS])
                    .execute(&mut *conn1)
                    .await?;

                let (export_fut, readers) = $export.build(&mut conn1).await?;

                let (import_fut, writers) = $import.build(&mut conn2).await?;

                let transport_futs = iter::zip(readers, writers).map($proc);

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
        }
    };

    (single: $name:literal, $export:expr, $import:expr, $(#[$attr:meta]),*) => {
        test_threaded_etl!("single_threaded", $name, |(r,w)|  pipe(r, w), $export, $import, $(#[$attr]),*);
    };

    (multi: $name:literal, $export:expr, $import:expr, $(#[$attr:meta]),*) => {
        test_threaded_etl!("multi_threaded", $name, |(r,w)|  tokio::spawn(pipe(r, w)).map_err(From::from).and_then(|r| async { r }), $export, $import, $(#[$attr]),*);
    }
}

    pub(crate) use test_threaded_etl;
}
