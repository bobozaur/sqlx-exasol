#![cfg(feature = "migrate")]

use sqlx_exasol::{
    error::BoxDynError, pool::PoolConnection, Column, Exasol, Executor, SqlStr, Type, TypeInfo,
};

#[sqlx_exasol::test(migrations = "tests/setup")]
async fn it_describes_columns(mut conn: PoolConnection<Exasol>) -> Result<(), BoxDynError> {
    let d = conn
        .describe(SqlStr::from_static("SELECT * FROM tweet"))
        .await?;

    assert_eq!(d.columns()[0].name(), "id");
    assert_eq!(d.columns()[1].name(), "created_at");
    assert_eq!(d.columns()[2].name(), "text");
    assert_eq!(d.columns()[3].name(), "owner_id");

    assert_eq!(d.nullable(0), None);
    assert_eq!(d.nullable(1), None);
    assert_eq!(d.nullable(2), None);
    assert_eq!(d.nullable(3), None);

    assert_eq!(d.columns()[0].type_info().name(), "DECIMAL(20, 0)");
    assert_eq!(d.columns()[1].type_info().name(), "TIMESTAMP");
    assert_eq!(d.columns()[2].type_info().name(), "VARCHAR(2000000) UTF8");
    assert_eq!(d.columns()[3].type_info().name(), "DECIMAL(20, 0)");

    Ok(())
}

#[sqlx_exasol::test(migrations = "tests/setup")]
async fn it_describes_params(mut conn: PoolConnection<Exasol>) -> Result<(), BoxDynError> {
    conn.execute(
        r"
CREATE TABLE with_hashtype_and_tinyint (
    id INT IDENTITY PRIMARY KEY,
    value_hashtype_1 HASHTYPE(1 BYTE),
    value_bool BOOLEAN,
    hashtype_n HASHTYPE(8 BYTE),
    value_int TINYINT,
    geo GEOMETRY
);
    ",
    )
    .await?;

    let d = conn
        .describe(SqlStr::from_static(
            "INSERT INTO with_hashtype_and_tinyint VALUES (?, ?, ?, ?, ?, ?);",
        ))
        .await?;

    let parameters = d.parameters().unwrap().unwrap_left();

    assert_eq!(parameters[0].name(), "DECIMAL(18, 0)");
    assert_eq!(parameters[1].name(), "HASHTYPE(1 BYTE)");
    assert_eq!(parameters[2].name(), "BOOLEAN");
    assert_eq!(parameters[3].name(), "HASHTYPE(8 BYTE)");
    assert_eq!(parameters[4].name(), "DECIMAL(3, 0)");
    // Undocumented inconsistent behavior.
    // See <https://github.com/exasol/websocket-api/issues/39>.
    assert_eq!(parameters[5].name(), "VARCHAR(2000000) UTF8");

    Ok(())
}

#[sqlx_exasol::test(migrations = "tests/setup")]
async fn it_describes_columns_and_params(
    mut conn: PoolConnection<Exasol>,
) -> Result<(), BoxDynError> {
    conn.execute(
        r"
CREATE TABLE with_hashtype_and_tinyint (
    id INT IDENTITY PRIMARY KEY,
    value_hashtype_1 HASHTYPE(1 BYTE),
    value_bool BOOLEAN,
    hashtype_n HASHTYPE(8 BYTE),
    value_int TINYINT,
    geo GEOMETRY
);
    ",
    )
    .await?;

    let d = conn
        .describe(SqlStr::from_static(
            "SELECT * FROM with_hashtype_and_tinyint WHERE value_hashtype_1 = ? AND value_int = ?;",
        ))
        .await?;

    assert_eq!(d.columns()[0].name(), "id");
    assert_eq!(d.columns()[1].name(), "value_hashtype_1");
    assert_eq!(d.columns()[2].name(), "value_bool");
    assert_eq!(d.columns()[3].name(), "hashtype_n");
    assert_eq!(d.columns()[4].name(), "value_int");
    assert_eq!(d.columns()[5].name(), "geo");

    assert_eq!(d.nullable(0), None);
    assert_eq!(d.nullable(1), None);
    assert_eq!(d.nullable(2), None);
    assert_eq!(d.nullable(3), None);
    assert_eq!(d.nullable(4), None);
    assert_eq!(d.nullable(5), None);

    assert_eq!(d.columns()[0].type_info().name(), "DECIMAL(18, 0)");
    assert_eq!(d.columns()[1].type_info().name(), "HASHTYPE(1 BYTE)");
    assert_eq!(d.columns()[2].type_info().name(), "BOOLEAN");
    assert_eq!(d.columns()[3].type_info().name(), "HASHTYPE(8 BYTE)");
    assert_eq!(d.columns()[4].type_info().name(), "DECIMAL(3, 0)");
    assert_eq!(d.columns()[5].type_info().name(), "GEOMETRY(0)");

    let parameters = d.parameters().unwrap().unwrap_left();

    assert_eq!(parameters[0].name(), "HASHTYPE(1 BYTE)");
    assert_eq!(parameters[1].name(), "DECIMAL(3, 0)");

    Ok(())
}

#[sqlx_exasol::test(migrations = "tests/setup")]
async fn test_boolean(mut conn: PoolConnection<Exasol>) -> Result<(), BoxDynError> {
    conn.execute(
        r"
CREATE TABLE with_hashtype_and_tinyint (
    id INT IDENTITY PRIMARY KEY,
    value_hashtype_1 HASHTYPE(1 BYTE),
    value_bool BOOLEAN,
    hashtype_n HASHTYPE(8 BYTE),
    value_int TINYINT
);
    ",
    )
    .await?;

    let d = conn
        .describe(SqlStr::from_static(
            "SELECT * FROM with_hashtype_and_tinyint",
        ))
        .await?;

    assert_eq!(d.column(2).name(), "value_bool");
    assert_eq!(d.column(2).type_info().name(), "BOOLEAN");

    assert_eq!(d.column(1).name(), "value_hashtype_1");
    assert_eq!(d.column(1).type_info().name(), "HASHTYPE(1 BYTE)");

    assert!(<bool as Type<Exasol>>::compatible(d.column(2).type_info()));

    Ok(())
}

#[sqlx_exasol::test(migrations = "tests/setup")]
async fn uses_alias_name(mut conn: PoolConnection<Exasol>) -> Result<(), BoxDynError> {
    let d = conn
        .describe(SqlStr::from_static("SELECT text AS tweet_text FROM tweet"))
        .await?;

    assert_eq!(d.columns()[0].name(), "tweet_text");

    Ok(())
}
