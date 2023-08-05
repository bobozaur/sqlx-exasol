use exasol::Exasol;
use sqlx::{query, query_as, query_scalar, Executor, FromRow};
use sqlx_core::{error::BoxDynError, pool::PoolConnection};

const SOME_F32: f32 = 51_231.727;
const SOME_F64: f64 = 651_231.727_376_231;
const SOME_OTHER_F64: f64 = 234_351_231.727_364;

#[sqlx::test]
async fn test_f32(mut con: PoolConnection<Exasol>) -> Result<(), BoxDynError> {
    #[derive(Clone, Copy, Debug, FromRow, PartialEq)]
    struct Row32 {
        col: f32,
        num: f32,
    }

    #[derive(Clone, Copy, Debug, FromRow, PartialEq)]
    struct Row64 {
        col: f64,
        num: f32,
    }

    con.execute("CREATE TABLE test_float ( col DECIMAL(36, 3), num DOUBLE )")
        .await?;

    let res = query("INSERT INTO test_float VALUES (?, ?)")
        .bind(SOME_F32)
        .bind(f32::MIN)
        .execute(&mut *con)
        .await?;

    assert_eq!(res.rows_affected(), 1);

    let res = query("INSERT INTO test_float VALUES (?, ?)")
        .bind(SOME_F32)
        .bind(f32::MAX)
        .execute(&mut *con)
        .await?;

    assert_eq!(res.rows_affected(), 1);

    let res: Vec<Row32> = query_as("SELECT * FROM test_float")
        .fetch_all(&mut *con)
        .await?;

    let res: Vec<_> = res
        .into_iter()
        .map(|r| Row32 {
            col: r.col.floor(),
            num: r.num.floor(),
        })
        .collect();

    let row1 = Row32 {
        col: SOME_F32.floor(),
        num: f32::MIN.floor(),
    };

    let row2 = Row32 {
        col: SOME_F32.floor(),
        num: f32::MAX.floor(),
    };

    assert_eq!(res, vec![row1, row2]);

    let res: Vec<Row64> = query_as("SELECT * FROM test_float")
        .fetch_all(&mut *con)
        .await?;

    let res: Vec<_> = res
        .into_iter()
        .map(|r| Row64 {
            col: r.col.floor(),
            num: r.num.floor(),
        })
        .collect();

    let row1 = Row64 {
        col: f64::from(SOME_F32.floor()),
        num: f32::MIN.floor(),
    };

    let row2 = Row64 {
        col: f64::from(SOME_F32.floor()),
        num: f32::MAX.floor(),
    };

    assert_eq!(res, vec![row1, row2]);

    let res = query("INSERT INTO test_float VALUES (?, ?)")
        .bind(0)
        .bind(0)
        .execute(&mut *con)
        .await;

    assert!(matches!(res, Err(sqlx::Error::Protocol(_))));

    let res = query_scalar::<Exasol, u64>("SELECT * FROM test_float")
        .fetch_all(&mut *con)
        .await;

    assert!(res.is_err());

    Ok(())
}
#[sqlx::test]
async fn test_f64(mut con: PoolConnection<Exasol>) -> Result<(), BoxDynError> {
    #[derive(Clone, Copy, Debug, FromRow, PartialEq)]
    struct Row64 {
        col: f64,
        num: f64,
    }
    con.execute("CREATE TABLE test_float ( col DECIMAL(36, 12), num DOUBLE )")
        .await?;

    let res = query("INSERT INTO test_float VALUES (?, ?)")
        .bind(SOME_F64)
        .bind(SOME_OTHER_F64 * -1.0)
        .execute(&mut *con)
        .await?;

    assert_eq!(res.rows_affected(), 1);

    let res = query("INSERT INTO test_float VALUES (?, ?)")
        .bind(SOME_F64)
        .bind(SOME_OTHER_F64)
        .execute(&mut *con)
        .await?;

    assert_eq!(res.rows_affected(), 1);

    let res: Vec<Row64> = query_as("SELECT * FROM test_float")
        .fetch_all(&mut *con)
        .await?;

    let row1 = Row64 {
        col: SOME_F64,
        num: SOME_OTHER_F64 * -1.0,
    };

    let row2 = Row64 {
        col: SOME_F64,
        num: SOME_OTHER_F64,
    };
    assert_eq!(res, vec![row1, row2]);

    let res = query("INSERT INTO test_float VALUES (?, ?)")
        .bind(0)
        .bind(0)
        .execute(&mut *con)
        .await;

    assert!(matches!(res, Err(sqlx::Error::Protocol(_))));

    let res = query_scalar::<Exasol, u64>("SELECT * FROM test_float")
        .fetch_all(&mut *con)
        .await;

    assert!(res.is_err());

    Ok(())
}
