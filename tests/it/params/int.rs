use exasol::Exasol;
use sqlx::{query, query_scalar, Executor};
use sqlx_core::{error::BoxDynError, pool::PoolConnection};

#[sqlx::test]
async fn test_i8(mut con: PoolConnection<Exasol>) -> Result<(), BoxDynError> {
    con.execute("CREATE TABLE test_int ( col DECIMAL(3, 0) )")
        .await?;

    let res = query("INSERT INTO test_int VALUES (?)")
        .bind(i8::MIN)
        .execute(&mut *con)
        .await?;

    assert_eq!(res.rows_affected(), 1);

    let res = query("INSERT INTO test_int VALUES (?)")
        .bind(i8::MAX)
        .execute(&mut *con)
        .await?;

    assert_eq!(res.rows_affected(), 1);

    let res: Vec<i8> = query_scalar("SELECT * FROM test_int")
        .fetch_all(&mut *con)
        .await?;

    assert_eq!(res, vec![i8::MIN, i8::MAX]);

    let res: Vec<i16> = query_scalar("SELECT * FROM test_int")
        .fetch_all(&mut *con)
        .await?;

    assert_eq!(res, vec![i16::from(i8::MIN), i16::from(i8::MAX)]);

    let res: Vec<i32> = query_scalar("SELECT * FROM test_int")
        .fetch_all(&mut *con)
        .await?;

    assert_eq!(res, vec![i32::from(i8::MIN), i32::from(i8::MAX)]);

    let res: Vec<i64> = query_scalar("SELECT * FROM test_int")
        .fetch_all(&mut *con)
        .await?;

    assert_eq!(res, vec![i64::from(i8::MIN), i64::from(i8::MAX)]);

    let res = query("INSERT INTO test_int VALUES (?)")
        .bind(i16::MAX)
        .execute(&mut *con)
        .await;

    assert!(matches!(res, Err(sqlx::Error::Protocol(_))));

    let res = query("INSERT INTO test_int VALUES (?)")
        .bind(i32::MAX)
        .execute(&mut *con)
        .await;

    assert!(matches!(res, Err(sqlx::Error::Protocol(_))));

    let res = query("INSERT INTO test_int VALUES (?)")
        .bind(i64::MAX)
        .execute(&mut *con)
        .await;

    assert!(matches!(res, Err(sqlx::Error::Protocol(_))));

    let res = query("INSERT INTO test_int VALUES (?)")
        .bind(f32::MIN)
        .execute(&mut *con)
        .await;

    assert!(matches!(res, Err(sqlx::Error::Protocol(_))));

    let res = query("INSERT INTO test_int VALUES (?)")
        .bind(f32::MAX)
        .execute(&mut *con)
        .await;

    assert!(matches!(res, Err(sqlx::Error::Protocol(_))));

    let res = query("INSERT INTO test_int VALUES (?)")
        .bind(f64::MIN)
        .execute(&mut *con)
        .await;

    assert!(matches!(res, Err(sqlx::Error::Protocol(_))));

    let res = query("INSERT INTO test_int VALUES (?)")
        .bind(f64::MAX)
        .execute(&mut *con)
        .await;

    assert!(matches!(res, Err(sqlx::Error::Protocol(_))));

    Ok(())
}

#[sqlx::test]
async fn test_i16(mut con: PoolConnection<Exasol>) -> Result<(), BoxDynError> {
    con.execute("CREATE TABLE test_int ( col DECIMAL(5, 0) )")
        .await?;

    let res = query("INSERT INTO test_int VALUES (?)")
        .bind(i16::MIN)
        .execute(&mut *con)
        .await?;

    assert_eq!(res.rows_affected(), 1);

    let res = query("INSERT INTO test_int VALUES (?)")
        .bind(i16::MAX)
        .execute(&mut *con)
        .await?;

    assert_eq!(res.rows_affected(), 1);

    let res: Vec<i16> = query_scalar("SELECT * FROM test_int")
        .fetch_all(&mut *con)
        .await?;

    assert_eq!(res, vec![i16::MIN, i16::MAX]);

    let res: Vec<i32> = query_scalar("SELECT * FROM test_int")
        .fetch_all(&mut *con)
        .await?;

    assert_eq!(res, vec![i32::from(i16::MIN), i32::from(i16::MAX)]);

    let res: Vec<i64> = query_scalar("SELECT * FROM test_int")
        .fetch_all(&mut *con)
        .await?;

    assert_eq!(res, vec![i64::from(i16::MIN), i64::from(i16::MAX)]);

    let res = query("INSERT INTO test_int VALUES (?)")
        .bind(i32::MAX)
        .execute(&mut *con)
        .await;

    assert!(matches!(res, Err(sqlx::Error::Protocol(_))));

    let res = query("INSERT INTO test_int VALUES (?)")
        .bind(i64::MAX)
        .execute(&mut *con)
        .await;

    assert!(matches!(res, Err(sqlx::Error::Protocol(_))));

    let res = query("INSERT INTO test_int VALUES (?)")
        .bind(f32::MIN)
        .execute(&mut *con)
        .await;

    assert!(matches!(res, Err(sqlx::Error::Protocol(_))));

    let res = query("INSERT INTO test_int VALUES (?)")
        .bind(f32::MAX)
        .execute(&mut *con)
        .await;

    assert!(matches!(res, Err(sqlx::Error::Protocol(_))));

    let res = query("INSERT INTO test_int VALUES (?)")
        .bind(f64::MIN)
        .execute(&mut *con)
        .await;

    assert!(matches!(res, Err(sqlx::Error::Protocol(_))));

    let res = query("INSERT INTO test_int VALUES (?)")
        .bind(f64::MAX)
        .execute(&mut *con)
        .await;

    assert!(matches!(res, Err(sqlx::Error::Protocol(_))));

    let res = query_scalar::<Exasol, i8>("SELECT * FROM test_int")
        .fetch_all(&mut *con)
        .await;

    assert!(res.is_err());

    Ok(())
}

#[sqlx::test]
async fn test_i32(mut con: PoolConnection<Exasol>) -> Result<(), BoxDynError> {
    con.execute("CREATE TABLE test_int ( col DECIMAL(10, 0) )")
        .await?;

    let res = query("INSERT INTO test_int VALUES (?)")
        .bind(i32::MIN)
        .execute(&mut *con)
        .await?;

    assert_eq!(res.rows_affected(), 1);

    let res = query("INSERT INTO test_int VALUES (?)")
        .bind(i32::MAX)
        .execute(&mut *con)
        .await?;

    assert_eq!(res.rows_affected(), 1);

    let res: Vec<i32> = query_scalar("SELECT * FROM test_int")
        .fetch_all(&mut *con)
        .await?;

    assert_eq!(res, vec![i32::MIN, i32::MAX]);

    let res: Vec<i64> = query_scalar("SELECT * FROM test_int")
        .fetch_all(&mut *con)
        .await?;

    assert_eq!(res, vec![i64::from(i32::MIN), i64::from(i32::MAX)]);

    let res = query("INSERT INTO test_int VALUES (?)")
        .bind(i64::MAX)
        .execute(&mut *con)
        .await;

    assert!(matches!(res, Err(sqlx::Error::Protocol(_))));

    let res = query("INSERT INTO test_int VALUES (?)")
        .bind(f32::MIN)
        .execute(&mut *con)
        .await;

    assert!(matches!(res, Err(sqlx::Error::Protocol(_))));

    let res = query("INSERT INTO test_int VALUES (?)")
        .bind(f32::MAX)
        .execute(&mut *con)
        .await;

    assert!(matches!(res, Err(sqlx::Error::Protocol(_))));

    let res = query("INSERT INTO test_int VALUES (?)")
        .bind(f64::MIN)
        .execute(&mut *con)
        .await;

    assert!(matches!(res, Err(sqlx::Error::Protocol(_))));

    let res = query("INSERT INTO test_int VALUES (?)")
        .bind(f64::MAX)
        .execute(&mut *con)
        .await;

    assert!(matches!(res, Err(sqlx::Error::Protocol(_))));

    let res = query_scalar::<Exasol, i8>("SELECT * FROM test_int")
        .fetch_all(&mut *con)
        .await;

    assert!(res.is_err());

    let res = query_scalar::<Exasol, i16>("SELECT * FROM test_int")
        .fetch_all(&mut *con)
        .await;

    assert!(res.is_err());

    Ok(())
}

#[sqlx::test]
async fn test_i64(mut con: PoolConnection<Exasol>) -> Result<(), BoxDynError> {
    con.execute("CREATE TABLE test_int ( col DECIMAL(20, 0) )")
        .await?;

    let res = query("INSERT INTO test_int VALUES (?)")
        .bind(i64::MIN)
        .execute(&mut *con)
        .await?;

    assert_eq!(res.rows_affected(), 1);

    let res = query("INSERT INTO test_int VALUES (?)")
        .bind(i64::MAX)
        .execute(&mut *con)
        .await?;

    assert_eq!(res.rows_affected(), 1);

    let res: Vec<i64> = query_scalar("SELECT * FROM test_int")
        .fetch_all(&mut *con)
        .await?;

    assert_eq!(res, vec![i64::MIN, i64::MAX]);

    let res = query("INSERT INTO test_int VALUES (?)")
        .bind(f32::MIN)
        .execute(&mut *con)
        .await;

    assert!(matches!(res, Err(sqlx::Error::Protocol(_))));

    let res = query("INSERT INTO test_int VALUES (?)")
        .bind(f32::MAX)
        .execute(&mut *con)
        .await;

    assert!(matches!(res, Err(sqlx::Error::Protocol(_))));

    let res = query("INSERT INTO test_int VALUES (?)")
        .bind(f64::MIN)
        .execute(&mut *con)
        .await;

    assert!(matches!(res, Err(sqlx::Error::Protocol(_))));

    let res = query("INSERT INTO test_int VALUES (?)")
        .bind(f64::MAX)
        .execute(&mut *con)
        .await;

    assert!(matches!(res, Err(sqlx::Error::Protocol(_))));

    let res = query_scalar::<Exasol, i8>("SELECT * FROM test_int")
        .fetch_all(&mut *con)
        .await;

    assert!(res.is_err());

    let res = query_scalar::<Exasol, i16>("SELECT * FROM test_int")
        .fetch_all(&mut *con)
        .await;

    assert!(res.is_err());

    let res = query_scalar::<Exasol, i32>("SELECT * FROM test_int")
        .fetch_all(&mut *con)
        .await;

    assert!(res.is_err());

    Ok(())
}
