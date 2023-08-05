use exasol::Exasol;
use sqlx::{query, query_scalar, Executor};
use sqlx_core::{error::BoxDynError, pool::PoolConnection};

#[sqlx::test]
async fn test_u8(mut con: PoolConnection<Exasol>) -> Result<(), BoxDynError> {
    con.execute("CREATE TABLE test_uint ( col DECIMAL(3, 0) )")
        .await?;

    let res = query("INSERT INTO test_uint VALUES (?)")
        .bind(u8::MIN)
        .execute(&mut *con)
        .await?;

    assert_eq!(res.rows_affected(), 1);

    let res = query("INSERT INTO test_uint VALUES (?)")
        .bind(u8::MAX)
        .execute(&mut *con)
        .await?;

    assert_eq!(res.rows_affected(), 1);

    let res: Vec<u8> = query_scalar("SELECT * FROM test_uint")
        .fetch_all(&mut *con)
        .await?;

    assert_eq!(res, vec![u8::MIN, u8::MAX]);

    let res: Vec<u16> = query_scalar("SELECT * FROM test_uint")
        .fetch_all(&mut *con)
        .await?;

    assert_eq!(res, vec![u16::from(u8::MIN), u16::from(u8::MAX)]);

    let res: Vec<u32> = query_scalar("SELECT * FROM test_uint")
        .fetch_all(&mut *con)
        .await?;

    assert_eq!(res, vec![u32::from(u8::MIN), u32::from(u8::MAX)]);

    let res: Vec<u64> = query_scalar("SELECT * FROM test_uint")
        .fetch_all(&mut *con)
        .await?;

    assert_eq!(res, vec![u64::from(u8::MIN), u64::from(u8::MAX)]);

    let res = query("INSERT INTO test_uint VALUES (?)")
        .bind(u16::MAX)
        .execute(&mut *con)
        .await;

    assert!(matches!(res, Err(sqlx::Error::Protocol(_))));

    let res = query("INSERT INTO test_uint VALUES (?)")
        .bind(u32::MAX)
        .execute(&mut *con)
        .await;

    assert!(matches!(res, Err(sqlx::Error::Protocol(_))));

    let res = query("INSERT INTO test_uint VALUES (?)")
        .bind(u64::MAX)
        .execute(&mut *con)
        .await;

    assert!(matches!(res, Err(sqlx::Error::Protocol(_))));

    let res = query("INSERT INTO test_uint VALUES (?)")
        .bind(f32::MIN)
        .execute(&mut *con)
        .await;

    assert!(matches!(res, Err(sqlx::Error::Protocol(_))));

    let res = query("INSERT INTO test_uint VALUES (?)")
        .bind(f32::MAX)
        .execute(&mut *con)
        .await;

    assert!(matches!(res, Err(sqlx::Error::Protocol(_))));

    let res = query("INSERT INTO test_uint VALUES (?)")
        .bind(f64::MIN)
        .execute(&mut *con)
        .await;

    assert!(matches!(res, Err(sqlx::Error::Protocol(_))));

    let res = query("INSERT INTO test_uint VALUES (?)")
        .bind(f64::MAX)
        .execute(&mut *con)
        .await;

    assert!(matches!(res, Err(sqlx::Error::Protocol(_))));

    Ok(())
}

#[sqlx::test]
async fn test_u16(mut con: PoolConnection<Exasol>) -> Result<(), BoxDynError> {
    con.execute("CREATE TABLE test_uint ( col DECIMAL(5, 0) )")
        .await?;

    let res = query("INSERT INTO test_uint VALUES (?)")
        .bind(u16::MIN)
        .execute(&mut *con)
        .await?;

    assert_eq!(res.rows_affected(), 1);

    let res = query("INSERT INTO test_uint VALUES (?)")
        .bind(u16::MAX)
        .execute(&mut *con)
        .await?;

    assert_eq!(res.rows_affected(), 1);

    let res: Vec<u16> = query_scalar("SELECT * FROM test_uint")
        .fetch_all(&mut *con)
        .await?;

    assert_eq!(res, vec![u16::MIN, u16::MAX]);

    let res: Vec<u32> = query_scalar("SELECT * FROM test_uint")
        .fetch_all(&mut *con)
        .await?;

    assert_eq!(res, vec![u32::from(u16::MIN), u32::from(u16::MAX)]);

    let res: Vec<u64> = query_scalar("SELECT * FROM test_uint")
        .fetch_all(&mut *con)
        .await?;

    assert_eq!(res, vec![u64::from(u16::MIN), u64::from(u16::MAX)]);

    let res = query("INSERT INTO test_uint VALUES (?)")
        .bind(u32::MAX)
        .execute(&mut *con)
        .await;

    assert!(matches!(res, Err(sqlx::Error::Protocol(_))));

    let res = query("INSERT INTO test_uint VALUES (?)")
        .bind(u64::MAX)
        .execute(&mut *con)
        .await;

    assert!(matches!(res, Err(sqlx::Error::Protocol(_))));

    let res = query("INSERT INTO test_uint VALUES (?)")
        .bind(f32::MIN)
        .execute(&mut *con)
        .await;

    assert!(matches!(res, Err(sqlx::Error::Protocol(_))));

    let res = query("INSERT INTO test_uint VALUES (?)")
        .bind(f32::MAX)
        .execute(&mut *con)
        .await;

    assert!(matches!(res, Err(sqlx::Error::Protocol(_))));

    let res = query("INSERT INTO test_uint VALUES (?)")
        .bind(f64::MIN)
        .execute(&mut *con)
        .await;

    assert!(matches!(res, Err(sqlx::Error::Protocol(_))));

    let res = query("INSERT INTO test_uint VALUES (?)")
        .bind(f64::MAX)
        .execute(&mut *con)
        .await;

    assert!(matches!(res, Err(sqlx::Error::Protocol(_))));

    let res = query_scalar::<Exasol, u8>("SELECT * FROM test_uint")
        .fetch_all(&mut *con)
        .await;

    assert!(res.is_err());

    Ok(())
}

#[sqlx::test]
async fn test_u32(mut con: PoolConnection<Exasol>) -> Result<(), BoxDynError> {
    con.execute("CREATE TABLE test_uint ( col DECIMAL(10, 0) )")
        .await?;

    let res = query("INSERT INTO test_uint VALUES (?)")
        .bind(u32::MIN)
        .execute(&mut *con)
        .await?;

    assert_eq!(res.rows_affected(), 1);

    let res = query("INSERT INTO test_uint VALUES (?)")
        .bind(u32::MAX)
        .execute(&mut *con)
        .await?;

    assert_eq!(res.rows_affected(), 1);

    let res: Vec<u32> = query_scalar("SELECT * FROM test_uint")
        .fetch_all(&mut *con)
        .await?;

    assert_eq!(res, vec![u32::MIN, u32::MAX]);

    let res: Vec<u64> = query_scalar("SELECT * FROM test_uint")
        .fetch_all(&mut *con)
        .await?;

    assert_eq!(res, vec![u64::from(u32::MIN), u64::from(u32::MAX)]);

    let res = query("INSERT INTO test_uint VALUES (?)")
        .bind(u64::MAX)
        .execute(&mut *con)
        .await;

    assert!(matches!(res, Err(sqlx::Error::Protocol(_))));

    let res = query("INSERT INTO test_uint VALUES (?)")
        .bind(f32::MIN)
        .execute(&mut *con)
        .await;

    assert!(matches!(res, Err(sqlx::Error::Protocol(_))));

    let res = query("INSERT INTO test_uint VALUES (?)")
        .bind(f32::MAX)
        .execute(&mut *con)
        .await;

    assert!(matches!(res, Err(sqlx::Error::Protocol(_))));

    let res = query("INSERT INTO test_uint VALUES (?)")
        .bind(f64::MIN)
        .execute(&mut *con)
        .await;

    assert!(matches!(res, Err(sqlx::Error::Protocol(_))));

    let res = query("INSERT INTO test_uint VALUES (?)")
        .bind(f64::MAX)
        .execute(&mut *con)
        .await;

    assert!(matches!(res, Err(sqlx::Error::Protocol(_))));

    let res = query_scalar::<Exasol, u8>("SELECT * FROM test_uint")
        .fetch_all(&mut *con)
        .await;

    assert!(res.is_err());

    let res = query_scalar::<Exasol, u16>("SELECT * FROM test_uint")
        .fetch_all(&mut *con)
        .await;

    assert!(res.is_err());

    Ok(())
}

#[sqlx::test]
async fn test_u64(mut con: PoolConnection<Exasol>) -> Result<(), BoxDynError> {
    con.execute("CREATE TABLE test_uint ( col DECIMAL(20, 0) )")
        .await?;

    let res = query("INSERT INTO test_uint VALUES (?)")
        .bind(u64::MIN)
        .execute(&mut *con)
        .await?;

    assert_eq!(res.rows_affected(), 1);

    let res = query("INSERT INTO test_uint VALUES (?)")
        .bind(u64::MAX)
        .execute(&mut *con)
        .await?;

    assert_eq!(res.rows_affected(), 1);

    let res: Vec<u64> = query_scalar("SELECT * FROM test_uint")
        .fetch_all(&mut *con)
        .await?;

    assert_eq!(res, vec![u64::MIN, u64::MAX]);

    let res = query("INSERT INTO test_uint VALUES (?)")
        .bind(f32::MIN)
        .execute(&mut *con)
        .await;

    assert!(matches!(res, Err(sqlx::Error::Protocol(_))));

    let res = query("INSERT INTO test_uint VALUES (?)")
        .bind(f32::MAX)
        .execute(&mut *con)
        .await;

    assert!(matches!(res, Err(sqlx::Error::Protocol(_))));

    let res = query("INSERT INTO test_uint VALUES (?)")
        .bind(f64::MIN)
        .execute(&mut *con)
        .await;

    assert!(matches!(res, Err(sqlx::Error::Protocol(_))));

    let res = query("INSERT INTO test_uint VALUES (?)")
        .bind(f64::MAX)
        .execute(&mut *con)
        .await;

    assert!(matches!(res, Err(sqlx::Error::Protocol(_))));

    let res = query_scalar::<Exasol, u8>("SELECT * FROM test_uint")
        .fetch_all(&mut *con)
        .await;

    assert!(res.is_err());

    let res = query_scalar::<Exasol, u16>("SELECT * FROM test_uint")
        .fetch_all(&mut *con)
        .await;

    assert!(res.is_err());

    let res = query_scalar::<Exasol, u32>("SELECT * FROM test_uint")
        .fetch_all(&mut *con)
        .await;

    assert!(res.is_err());

    Ok(())
}
