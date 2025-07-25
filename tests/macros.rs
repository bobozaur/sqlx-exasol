#![cfg(feature = "migrate")]

#[macro_export]
macro_rules! test_type_valid {
    ($name:ident<$ty:ty>::$datatype:literal::($($unprepared:expr => $prepared:expr),+)) => {
        paste::item! {
            #[sqlx_exasol::test]
            async fn [< test_type_valid_ $name >] (
                mut con: sqlx_exasol::pool::PoolConnection<sqlx_exasol::Exasol>,
            ) -> Result<(), sqlx_exasol::error::BoxDynError> {
                use sqlx_exasol::{Executor, query, query_scalar};

                let create_sql = concat!("CREATE TABLE sqlx_test_type ( col ", $datatype, " );");
                con.execute(create_sql).await?;

                $(
                    let query_result = query("INSERT INTO sqlx_test_type VALUES (?)")
                        .bind($prepared)
                        .execute(&mut *con)
                        .await?;

                    assert_eq!(query_result.rows_affected(), 1);
                    let query_str = format!("INSERT INTO sqlx_test_type VALUES (CAST ({} as {}));", $unprepared, $datatype);

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
            #[sqlx_exasol::test]
            async fn [< test_type_array_ $name >] (
                mut con: sqlx_exasol::pool::PoolConnection<sqlx_exasol::Exasol>,
            ) -> Result<(), sqlx_exasol::error::BoxDynError> {
                use sqlx_exasol::{Executor, query, query_scalar};

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

                    let rows_affected = usize::try_from(query_result.rows_affected()).unwrap();
                    assert_eq!(rows_affected, values.len());
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
                #[sqlx_exasol::test]
                async fn [< test_type_invalid_ $name >] (
                    mut con: sqlx_exasol::pool::PoolConnection<sqlx_exasol::Exasol>,
                ) -> Result<(), sqlx_exasol::error::BoxDynError> {
                    use sqlx_exasol::{Executor, query, query_scalar};

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
            #[sqlx_exasol::test]
            async fn [< test_etl_ $kind _ $name >](pool_opts: PoolOptions<Exasol>, exa_opts: ExaConnectOptions) -> anyhow::Result<()> {
                let pool = pool_opts.min_connections(2).connect_with(exa_opts).await?;

                let mut conn1 = pool.acquire().await?;
                let mut conn2 = pool.acquire().await?;

                conn1
                    .execute(concat!("CREATE TABLE ", $table, " ( col VARCHAR(200) );"))
                    .await?;

                sqlx_exasol::query(concat!("INSERT INTO ", $table, " VALUES (?)"))
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

                let num_rows: i64 = sqlx_exasol::query_scalar(concat!("SELECT COUNT(*) FROM ", $table))
                    .fetch_one(&mut *conn1)
                    .await?;

                assert_eq!(num_rows, 2 * NUM_ROWS as i64, "export + import rows");

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
            $crate::test_etl!("multi_threaded", $name, $num_workers, $table, |(r,w)|  sqlx_exasol::__rt::spawn(pipe(r, w)), $export, $import, $(#[$attr]),*);
        }
    }

#[macro_export]
macro_rules! test_compile_time_type {
    ($col:ident, $ty:tt, $value:expr, $insert:expr, $select:expr) => {
        paste::item! {
            #[ignore]
            #[sqlx_exasol::test(migrations = "tests/migrations_compile_time")]
            async fn [< test_compile_time_ $col >](
                mut conn: sqlx_exasol::pool::PoolConnection<sqlx_exasol::Exasol>,
            ) -> anyhow::Result<()> {
                use sqlx_exasol::types::ExaIter;

                let value = $value;

                sqlx_exasol::query!($insert, value.clone())
                .execute(&mut *conn)
                .await?;

                sqlx_exasol::query!($insert, &value)
                .execute(&mut *conn)
                .await?;

                sqlx_exasol::query!($insert, Some(value.clone()))
                .execute(&mut *conn)
                .await?;

                sqlx_exasol::query!($insert, Some(&value))
                .execute(&mut *conn)
                .await?;

                sqlx_exasol::query!($insert, vec![value.clone()])
                .execute(&mut *conn)
                .await?;

                sqlx_exasol::query!($insert, vec![&value])
                .execute(&mut *conn)
                .await?;

                sqlx_exasol::query!($insert, vec![Some(value.clone())])
                .execute(&mut *conn)
                .await?;

                sqlx_exasol::query!($insert, vec![Some(&value)])
                .execute(&mut *conn)
                .await?;

                let arr = [value.clone(); 1];
                sqlx_exasol::query!($insert, arr.as_slice())
                .execute(&mut *conn)
                .await?;

                sqlx_exasol::query!($insert, ExaIter::new(arr.iter().filter(|_| true)))
                .execute(&mut *conn)
                .await?;

                sqlx_exasol::query!($insert, arr)
                .execute(&mut *conn)
                .await?;

                let arr_ref = [&value; 1];
                sqlx_exasol::query!($insert, arr_ref.as_slice())
                .execute(&mut *conn)
                .await?;

                sqlx_exasol::query!($insert, ExaIter::new(arr_ref.iter().filter(|_| true).map(|v| *v)))
                .execute(&mut *conn)
                .await?;

                let arr_ref = [&value; 1];
                sqlx_exasol::query!($insert, arr_ref)
                .execute(&mut *conn)
                .await?;

                let opt_arr = [Some(value.clone()); 1];
                sqlx_exasol::query!($insert, opt_arr.as_slice())
                .execute(&mut *conn)
                .await?;

                sqlx_exasol::query!($insert, ExaIter::new(opt_arr.iter().filter(|_| true)))
                .execute(&mut *conn)
                .await?;

                sqlx_exasol::query!($insert, opt_arr)
                .execute(&mut *conn)
                .await?;

                let opt_arr_ref = [Some(&value); 1];
                sqlx_exasol::query!($insert, opt_arr_ref.as_slice())
                .execute(&mut *conn)
                .await?;

                sqlx_exasol::query!($insert, ExaIter::new(opt_arr_ref.iter().filter(|_| true)))
                .execute(&mut *conn)
                .await?;

                sqlx_exasol::query!($insert, opt_arr_ref)
                .execute(&mut *conn)
                .await?;

                let _: Vec<Option<$ty>> = sqlx_exasol::query_scalar!($select)
                .fetch_all(&mut *conn)
                .await?;

                Ok(())
            }
        }
    };

    ($ty:tt, $value:expr, $insert:literal, $select:literal) => {
        test_compile_time_type!($ty, $ty, $value, $insert, $select);
    };
}
