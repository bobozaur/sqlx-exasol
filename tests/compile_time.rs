//! Command to generate offline query files:
//! ```shell
//! cargo run -p sqlx-exasol-cli prepare -- --features runtime-tokio --tests
//! ```

#[ignore]
#[sqlx_exasol::test(migrations = "tests/migrations_compile_time")]
async fn test_compile_time_queries(
    mut conn: sqlx_exasol::pool::PoolConnection<sqlx_exasol::Exasol>,
) -> anyhow::Result<()> {
    sqlx_exasol::query!(
        "INSERT INTO compile_time_tests (column_bool) VALUES(?);",
        true
    )
    .execute(&mut *conn)
    .await?;

    let _: bool = sqlx_exasol::query_scalar!(
        r#"SELECT column_bool as "column_bool!" FROM compile_time_tests WHERE column_bool IS NOT NULL"#,
    )
    .fetch_one(&mut *conn)
    .await?;

    sqlx_exasol::query!(
        "INSERT INTO compile_time_tests (column_i8) VALUES(?);",
        10i8
    )
    .execute(&mut *conn)
    .await?;
    
    let _: i8 = sqlx_exasol::query_scalar!(
        r#"SELECT column_i8 as "column_i8!" FROM compile_time_tests WHERE column_i8 IS NOT NULL"#,
    )
    .fetch_one(&mut *conn)
    .await?;

    sqlx_exasol::query!(
        "INSERT INTO compile_time_tests (column_i16) VALUES(?);",
        10i16
    )
    .execute(&mut *conn)
    .await?;
    
    let _: i16 = sqlx_exasol::query_scalar!(
        r#"SELECT column_i16 as "column_i16!" FROM compile_time_tests WHERE column_i16 IS NOT NULL"#,
    )
    .fetch_one(&mut *conn)
    .await?;

    sqlx_exasol::query!(
        "INSERT INTO compile_time_tests (column_i32) VALUES(?);",
        10i32
    )
    .execute(&mut *conn)
    .await?;
    
    let _: i32 = sqlx_exasol::query_scalar!(
        r#"SELECT column_i32 as "column_i32!" FROM compile_time_tests WHERE column_i32 IS NOT NULL"#,
    )
    .fetch_one(&mut *conn)
    .await?;

    sqlx_exasol::query!(
        "INSERT INTO compile_time_tests (column_i64) VALUES(?);",
        10i64
    )
    .execute(&mut *conn)
    .await?;

    let _: i64 = sqlx_exasol::query_scalar!(
        r#"SELECT column_i64 as "column_i64!" FROM compile_time_tests WHERE column_i64 IS NOT NULL"#,
    )
    .fetch_one(&mut *conn)
    .await?;
    
    sqlx_exasol::query!(
        "INSERT INTO compile_time_tests (column_i128) VALUES(?);",
        10i128
    )
    .execute(&mut *conn)
    .await?;
    
    let _: i128 = sqlx_exasol::query_scalar!(
        r#"SELECT column_i128 as "column_i128!" FROM compile_time_tests WHERE column_i128 IS NOT NULL"#,
    )
    .fetch_one(&mut *conn)
    .await?;

    sqlx_exasol::query!(
        "INSERT INTO compile_time_tests (column_f64) VALUES(?);",
        15.3f64
    )
    .execute(&mut *conn)
    .await?;
    
    let _: f64 = sqlx_exasol::query_scalar!(
        r#"SELECT column_f64 as "column_f64!" FROM compile_time_tests WHERE column_f64 IS NOT NULL"#,
    )
    .fetch_one(&mut *conn)
    .await?;

    // #[cfg(feature = "chrono")]
    // {
    //     sqlx_exasol::query!(
    //         "INSERT INTO compile_time_tests (column_date) VALUES(?);",
    //         sqlx_exasol::types::chrono::NaiveDateTime::default().and_utc()
    //     )
    //     .execute(&mut *conn)
    //     .await?;
    // }

    // struct User {
    //     user_id: u64,
    //     username: String,
    // }

    // let username = "test";

    // sqlx_exasol::query!("INSERT INTO users (username) VALUES(?);", username)
    //     .execute(&mut *conn)
    //     .await?;

    // let user_id: u64 = sqlx_exasol::query_scalar!(
    //     r#"SELECT user_id as "user_id!" FROM users WHERE username = ?"#,
    //     username
    // )
    // .fetch_one(&mut *conn)
    // .await?;

    // let user = sqlx_exasol::query_as!(
    //     User,
    //     r#"SELECT user_id as "user_id!", username as "username!" FROM users WHERE user_id = ?"#,
    //     user_id
    // )
    // .fetch_one(&mut *conn)
    // .await?;

    // assert_eq!(user.user_id, user_id);
    // assert_eq!(user.username, username);

    Ok(())
}
