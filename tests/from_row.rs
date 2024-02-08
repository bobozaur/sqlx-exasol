#![cfg(feature = "migrate")]

use sqlx::{Executor, FromRow};
use sqlx_core::pool::PoolConnection;
use sqlx_exasol::Exasol;

#[derive(Debug, FromRow, PartialEq, Eq)]
struct TestRow {
    name: String,
    age: u8,
    amount: u64,
}

#[sqlx::test]
async fn test_from_row(mut conn: PoolConnection<Exasol>) -> anyhow::Result<()> {
    conn.execute(
        r#"
        CREATE TABLE TEST_FROM_ROW (
            name VARCHAR(200), 
            age DECIMAL(3, 0), 
            amount DECIMAL(15, 0)
         );"#,
    )
    .await?;

    let test_row1 = TestRow {
        name: "Lilo".to_owned(),
        age: 5,
        amount: 20,
    };

    let test_row2 = TestRow {
        name: "Stitch".to_owned(),
        age: 123,
        amount: 43759384749,
    };

    sqlx::query("INSERT INTO TEST_FROM_ROW VALUES (?, ?, ?)")
        .bind([&test_row1.name, &test_row2.name])
        .bind([&test_row1.age, &test_row2.age])
        .bind([&test_row1.amount, &test_row2.amount])
        .execute(&mut *conn)
        .await?;

    let rows: Vec<TestRow> = sqlx::query_as("SELECT * FROM TEST_FROM_ROW ORDER BY age")
        .fetch_all(&mut *conn)
        .await?;

    assert_eq!(rows, vec![test_row1, test_row2]);

    Ok(())
}
