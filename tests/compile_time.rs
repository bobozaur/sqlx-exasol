//! Command to generate offline query files:
//! ```shell
//! cargo run -p sqlx-exasol-cli prepare -- --features runtime-tokio --tests
//! ```

#[sqlx_exasol::test(migrations = "tests/migrations")]
#[ignore]
async fn test_compile_time_queries(
    mut conn: sqlx_exasol::pool::PoolConnection<sqlx_exasol::Exasol>,
) -> anyhow::Result<()> {
    struct User {
        user_id: u64,
        username: String,
    }

    let username = "test";

    sqlx_exasol::query!("INSERT INTO users (username) VALUES(?);", username)
        .execute(&mut *conn)
        .await?;

    let user_id: u64 = sqlx_exasol::query_scalar!(
        r#"SELECT user_id as "user_id!" FROM users WHERE username = ?"#,
        username
    )
    .fetch_one(&mut *conn)
    .await?;

    let user = sqlx_exasol::query_as!(
        User,
        r#"SELECT user_id as "user_id!", username as "username!" FROM users WHERE user_id = ?"#,
        user_id
    )
    .fetch_one(&mut *conn)
    .await?;

    assert_eq!(user.user_id, user_id);
    assert_eq!(user.username, username);

    Ok(())
}
