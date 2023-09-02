mod bool;
#[cfg(feature = "chrono")]
mod chrono;
mod float;
mod geometry;
mod int;
mod invalid;
#[cfg(feature = "rust_decimal")]
mod rust_decimal;
mod str;
mod uint;
#[cfg(feature = "uuid")]
mod uuid;

use macros::test_type_array;
use macros::test_type_invalid;
use macros::test_type_valid;

#[sqlx::test]
async fn test_equal_arrays(
    mut con: sqlx_core::pool::PoolConnection<sqlx_exasol::Exasol>,
) -> Result<(), sqlx_core::error::BoxDynError> {
    use sqlx_core::{executor::Executor, query::query, query_as::query_as};
    use std::iter::zip;

    con.execute(
        "CREATE TABLE sqlx_test_type ( col1 BOOLEAN, col2 DECIMAL(10, 0), col3 VARCHAR(100) );",
    )
    .await?;

    let bools = vec![false, true, false];
    let ints = vec![1, 2, 3];
    let mut strings = vec![Some("one".to_owned()), None, Some(String::new())];

    let query_result = query("INSERT INTO sqlx_test_type VALUES (?, ?, ?)")
        .bind(&bools)
        .bind(&ints)
        .bind(&strings)
        .execute(&mut *con)
        .await?;

    assert_eq!(query_result.rows_affected(), 3);

    let values: Vec<(bool, u32, Option<String>)> =
        query_as("SELECT * FROM sqlx_test_type ORDER BY col2;")
            .fetch_all(&mut *con)
            .await?;

    // Exasol treats empty strings as NULL
    strings.pop();
    strings.push(None);

    let expected = zip(zip(bools, ints), strings).map(|((b, i), s)| (b, i, s));
    for (v, e) in zip(values, expected) {
        assert_eq!(v, e);
    }

    Ok(())
}

#[sqlx::test]
async fn test_unequal_arrays(
    mut con: sqlx_core::pool::PoolConnection<sqlx_exasol::Exasol>,
) -> Result<(), sqlx_core::error::BoxDynError> {
    use sqlx_core::{executor::Executor, query::query};

    con.execute(
        "CREATE TABLE sqlx_test_type ( col1 BOOLEAN, col2 DECIMAL(10, 0), col3 VARCHAR(100) );",
    )
    .await?;

    let bools = vec![false, true, false];
    let ints = vec![1, 2, 3, 4];
    let strings = vec![Some("one".to_owned()), Some(String::new())];

    query("INSERT INTO sqlx_test_type VALUES (?, ?, ?)")
        .bind(&bools)
        .bind(&ints)
        .bind(&strings)
        .execute(&mut *con)
        .await
        .unwrap_err();

    Ok(())
}

#[sqlx::test]
async fn test_exceeding_arrays(
    mut con: sqlx_core::pool::PoolConnection<sqlx_exasol::Exasol>,
) -> Result<(), sqlx_core::error::BoxDynError> {
    use sqlx_core::{executor::Executor, query::query};

    con.execute(
        "CREATE TABLE sqlx_test_type ( col1 BOOLEAN, col2 DECIMAL(10, 0), col3 VARCHAR(100) );",
    )
    .await?;

    let bools = vec![false, true, false];
    let ints = vec![1, 2, u64::MAX];
    let strings = vec![Some("one".to_owned()), Some(String::new()), None];

    query("INSERT INTO sqlx_test_type VALUES (?, ?, ?)")
        .bind(&bools)
        .bind(&ints)
        .bind(&strings)
        .execute(&mut *con)
        .await
        .unwrap_err();

    Ok(())
}

#[sqlx::test]
async fn test_decode_error(
    mut con: sqlx_core::pool::PoolConnection<sqlx_exasol::Exasol>,
) -> Result<(), sqlx_core::error::BoxDynError> {
    use sqlx_core::{executor::Executor, query::query, query_scalar::query_scalar};

    con.execute("CREATE TABLE sqlx_test_type ( col DECIMAL(10, 0) );")
        .await?;

    query("INSERT INTO sqlx_test_type VALUES (?)")
        .bind(u32::MAX)
        .execute(&mut *con)
        .await?;

    let error = query_scalar::<_, u8>("SELECT col FROM sqlx_test_type")
        .fetch_one(&mut *con)
        .await
        .unwrap_err();

    eprintln!("{error}");

    Ok(())
}

mod macros {
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
        $crate::types::test_type_valid!($name<$ty>::$datatype::($($unprepared => $unprepared),+));
    };

    ($name:ident::$datatype:literal::($($unprepared:expr => $prepared:expr),+)) => {
        $crate::types::test_type_valid!($name<$name>::$datatype::($($unprepared => $prepared),+));
    };

    ($name:ident::$datatype:literal::($($unprepared:expr),+)) => {
        $crate::types::test_type_valid!($name::$datatype::($($unprepared => $unprepared),+));
    };
}

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

    pub(crate) use test_type_array;
    pub(crate) use test_type_invalid;
    pub(crate) use test_type_valid;
}
