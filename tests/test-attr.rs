#![cfg(feature = "migrate")]

use sqlx_exasol::ExaPool;

const MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("tests/migrations");

#[sqlx::test]
async fn it_gets_a_pool(pool: ExaPool) -> sqlx::Result<()> {
    let mut conn = pool.acquire().await?;

    let db_name: String = sqlx::query_scalar("SELECT CURRENT_SCHEMA")
        .fetch_one(&mut *conn)
        .await?;

    assert!(db_name.starts_with("_sqlx_test_"), "db_name: {db_name:?}",);

    Ok(())
}

// This should apply migrations and then `fixtures/users.sql`
#[sqlx::test(migrations = "tests/migrations", fixtures("users"))]
async fn it_gets_users(pool: ExaPool) -> sqlx::Result<()> {
    let usernames: Vec<String> =
        sqlx::query_scalar(r"SELECT username FROM users ORDER BY username")
            .fetch_all(&pool)
            .await?;

    assert_eq!(usernames, ["alice", "bob"]);

    let post_exists: bool = sqlx::query_scalar("SELECT exists(SELECT 1 FROM post)")
        .fetch_one(&pool)
        .await?;

    assert!(!post_exists);

    let comment_exists: bool = sqlx::query_scalar("SELECT exists(SELECT 1 FROM comment)")
        .fetch_one(&pool)
        .await?;

    assert!(!comment_exists);

    Ok(())
}

#[sqlx::test(migrations = "tests/migrations", fixtures("users", "posts"))]
async fn it_gets_posts(pool: ExaPool) -> sqlx::Result<()> {
    let post_contents: Vec<String> =
        sqlx::query_scalar("SELECT content FROM post ORDER BY created_at")
            .fetch_all(&pool)
            .await?;

    assert_eq!(
        post_contents,
        [
            "This new computer is lightning-fast!",
            "@alice is a haxxor :("
        ]
    );

    let comment_exists: bool = sqlx::query_scalar("SELECT exists(SELECT 1 FROM comment)")
        .fetch_one(&pool)
        .await?;

    assert!(!comment_exists);

    Ok(())
}

// Try `migrator`
#[sqlx::test(migrator = "MIGRATOR", fixtures("users", "posts", "comments"))]
async fn it_gets_comments(pool: ExaPool) -> sqlx::Result<()> {
    let post_1_comments: Vec<String> =
        sqlx::query_scalar("SELECT content FROM comment WHERE post_id = ? ORDER BY created_at")
            .bind(1)
            .fetch_all(&pool)
            .await?;

    assert_eq!(
        post_1_comments,
        ["lol bet ur still bad, 1v1 me", "you're on!"]
    );

    let post_2_comments: Vec<String> =
        sqlx::query_scalar("SELECT content FROM comment WHERE post_id = ? ORDER BY created_at")
            .bind(2)
            .fetch_all(&pool)
            .await?;

    assert_eq!(post_2_comments, ["lol you're just mad you lost :P"]);

    Ok(())
}
