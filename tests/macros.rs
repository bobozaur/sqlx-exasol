#[macro_export]
macro_rules! test_type_valid {
    ($name:ident<$ty:ty>::$datatype:literal::($($unprepared:expr => $prepared:expr),+)) => {
        paste::item! {
            #[sqlx::test]
            async fn [< test_type_valid_ $name >] (
                mut con: sqlx_core::pool::PoolConnection<sqlx_exasol::Exasol>,
            ) -> Result<(), sqlx_core::error::BoxDynError> {
                use sqlx_core::{executor::Executor, query::query, query_scalar::query_scalar};

                let create_sql = concat!("CREATE TABLE sqlx_test_type ( col ", $datatype, " );");
                con.execute(create_sql).await?;

                $(
                    let query_result = query("INSERT INTO sqlx_test_type VALUES (?)")
                        .bind($prepared)
                        .execute(&mut *con)
                        .await?;

                    assert_eq!(query_result.rows_affected(), 1);
                    let query_str = format!("INSERT INTO sqlx_test_type VALUES (CAST ({} as {}));", $unprepared, $datatype);
                    eprintln!("{query_str}");

                    let query_result = con.execute(query_str.as_str()).await?;

                    assert_eq!(query_result.rows_affected(), 1);

                    let mut values: Vec<$ty> = query_scalar("SELECT * FROM sqlx_test_type;")
                        .fetch_all(&mut *con)
                        .await?;

                    let first_value = values.pop().unwrap();
                    let second_value = values.pop().unwrap();

                    assert_eq!(first_value, second_value, "prepared and unprepared types");
                    assert_eq!(first_value, $prepared, "provided and expected values");
                    assert_eq!(second_value, $prepared, "provided and expected values");

                    con.execute("DELETE FROM sqlx_test_type;").await?;
                )+

                Ok(())
            }
        }
    };

    ($name:ident<$ty:ty>::$datatype:literal::($($unprepared:expr),+)) => {
        $crate::test_type_valid!($name<$ty>::$datatype::($($unprepared => $unprepared),+));
    };

    ($name:ident::$datatype:literal::($($unprepared:expr => $prepared:expr),+)) => {
        $crate::test_type_valid!($name<$name>::$datatype::($($unprepared => $prepared),+));
    };

    ($name:ident::$datatype:literal::($($unprepared:expr),+)) => {
        $crate::test_type_valid!($name::$datatype::($($unprepared => $unprepared),+));
    };
}

#[macro_export]
macro_rules! test_type_array {
    ($name:ident<$ty:ty>::$datatype:literal::($($prepared:expr),+)) => {
        paste::item! {
            #[sqlx::test]
            async fn [< test_type_array_ $name >] (
                mut con: sqlx_core::pool::PoolConnection<sqlx_exasol::Exasol>,
            ) -> Result<(), sqlx_core::error::BoxDynError> {
                use sqlx_core::{executor::Executor, query::query, query_scalar::query_scalar};

                let create_sql = concat!("CREATE TABLE sqlx_test_type ( col ", $datatype, " );");
                con.execute(create_sql).await?;

                $(
                    let query_result = query("INSERT INTO sqlx_test_type VALUES (?)")
                        .bind($prepared)
                        .execute(&mut *con)
                        .await?;

                    let values: Vec<$ty> = query_scalar("SELECT * FROM sqlx_test_type;")
                        .fetch_all(&mut *con)
                        .await?;

                    assert_eq!(query_result.rows_affected() as usize, values.len());
                    con.execute("DELETE FROM sqlx_test_type;").await?;
                )+

                Ok(())
            }
        }
    };
}

#[macro_export]
macro_rules! test_type_invalid {
        ($name:ident<$ty:ty>::$datatype:literal::($($prepared:expr),+)) => {
            paste::item! {
                #[sqlx::test]
                async fn [< test_type_invalid_ $name >] (
                    mut con: sqlx_core::pool::PoolConnection<sqlx_exasol::Exasol>,
                ) -> Result<(), sqlx_core::error::BoxDynError> {
                    use sqlx_core::{executor::Executor, query::query, query_scalar::query_scalar};

                    let create_sql = concat!("CREATE TABLE sqlx_test_type ( col ", $datatype, " );");
                    con.execute(create_sql).await?;

                    $(
                        let query_result = query("INSERT INTO sqlx_test_type VALUES (?)")
                            .bind($prepared)
                            .execute(&mut *con)
                            .await;

                        if let Err(e) = query_result {
                            eprintln!("Error inserting value: {e}");
                        } else {
                            let values_result: Result<Vec<$ty>, _> = query_scalar("SELECT * FROM sqlx_test_type;")
                                .fetch_all(&mut *con)
                                .await;

                            let error = values_result.unwrap_err();
                            eprintln!("Error retrieving value: {error}");

                            con.execute("DELETE FROM sqlx_test_type;").await?;
                        }
                    )+

                    Ok(())
                }
            }
        };

}

#[macro_export]
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
                let transport_futs = iter::zip(readers, writers).map($proc);

                let (export_res, import_res, _) =
                try_join3(export_fut.map_err(From::from), import_fut.map_err(From::from), try_join_all(transport_futs)).await.map_err(|e| anyhow::anyhow! {e})?;


                assert_eq!(NUM_ROWS as u64, export_res.rows_affected(), "exported rows");
                assert_eq!(NUM_ROWS as u64, import_res.rows_affected(), "imported rows");

                let num_rows: u64 = sqlx::query_scalar(concat!("SELECT COUNT(*) FROM ", $table))
                    .fetch_one(&mut *conn1)
                    .await?;

                assert_eq!(num_rows, 2 * NUM_ROWS as u64, "export + import rows");

                Ok(())
            }
        }
    };
}

#[macro_export]
macro_rules! test_etl_single_threaded {
        ($name:literal, $table:literal, $export:expr, $import:expr, $(#[$attr:meta]),*) => {
            $crate::test_etl_single_threaded!($name, 1, $table, $export, $import, $(#[$attr]),*);
        };

        ($name:literal, $num_workers:expr, $table:literal, $export:expr, $import:expr, $(#[$attr:meta]),*) => {
            $crate::test_etl!("single_threaded", $name, $num_workers, $table, |(r,w)|  pipe(r, w), $export, $import, $(#[$attr]),*);
        }
    }

#[macro_export]
macro_rules! test_etl_multi_threaded {
        ($name:literal, $table:literal, $export:expr, $import:expr, $(#[$attr:meta]),*) => {
            $crate::test_etl_multi_threaded!($name, 1, $table, $export, $import, $(#[$attr]),*);
        };

        ($name:literal, $num_workers:expr, $table:literal, $export:expr, $import:expr, $(#[$attr:meta]),*) => {
            $crate::test_etl!("multi_threaded", $name, $num_workers, $table, |(r,w)|  tokio::spawn(pipe(r, w)).map_err(From::from).and_then(|r| async { r }), $export, $import, $(#[$attr]),*);
        }
    }
