#![cfg(feature = "migrate")]
#![cfg(feature = "etl")]

use futures_util::{
    future::{try_join3, try_join_all},
    AsyncReadExt, AsyncWriteExt, TryFutureExt,
};
use sqlx_exasol::{
    error::BoxDynError,
    etl::{ExaExport, ExaImport, ExportBuilder, ExportSource, ImportBuilder},
    pool::PoolOptions,
    ConnectOptions, Connection, ExaConnectOptions, ExaConnection, Exasol, Executor,
};

#[ignore]
#[sqlx_exasol::test]
async fn it_works_with_io_combo_disabled(
    pool_opts: PoolOptions<Exasol>,
    exa_opts: ExaConnectOptions,
) -> Result<(), BoxDynError> {
    let mut url = exa_opts.to_url_lossy();

    url.query_pairs_mut()
        .append_pair("ssl-mode", "disabled")
        .append_pair("compression", "disabled");

    let exa_opts = ExaConnectOptions::from_url(&url)?;
    io_combo(pool_opts, exa_opts, false, false).await
}

#[ignore]
#[sqlx_exasol::test]
async fn it_works_with_io_combo_preferred(
    pool_opts: PoolOptions<Exasol>,
    exa_opts: ExaConnectOptions,
) -> Result<(), BoxDynError> {
    let compression_supported = cfg!(feature = "compression");
    let tls_supported = cfg!(any(
        feature = "tls-native-tls",
        feature = "tls-rustls-ring-webpki",
        feature = "tls-rustls-ring-native-roots"
    ));

    let mut url = exa_opts.to_url_lossy();

    url.query_pairs_mut()
        .append_pair("ssl-mode", "preferred")
        .append_pair("compression", "preferred");

    let exa_opts = ExaConnectOptions::from_url(&url)?;
    io_combo(pool_opts, exa_opts, tls_supported, compression_supported).await
}

#[ignore]
#[allow(unreachable_code, reason = "conditionally compiled")]
#[sqlx_exasol::test]
async fn it_works_with_io_combo_required(
    pool_opts: PoolOptions<Exasol>,
    exa_opts: ExaConnectOptions,
) -> Result<(), BoxDynError> {
    let mut url = exa_opts.to_url_lossy();

    url.query_pairs_mut()
        .append_pair("ssl-mode", "required")
        .append_pair("compression", "required");

    let exa_opts = ExaConnectOptions::from_url(&url)?;

    #[allow(unused_mut, reason = "conditionally compiled")]
    let mut res = io_combo(pool_opts, exa_opts, true, true).await;

    #[cfg(not(any(feature = "tls-native-tls", feature = "tls-rustls")))]
    {
        assert!(res.is_err());
        return Ok(());
    }

    #[cfg(not(feature = "compression"))]
    {
        assert!(res.is_err());
        return Ok(());
    }

    res
}

#[allow(clippy::cast_possible_wrap)]
async fn io_combo(
    pool_opts: PoolOptions<Exasol>,
    exa_opts: ExaConnectOptions,
    tls_expected: bool,
    compression_expected: bool,
) -> Result<(), BoxDynError> {
    let pool = pool_opts.min_connections(2).connect_with(exa_opts).await?;

    let mut conn1 = pool.acquire().await?;
    let mut conn2 = pool.acquire().await?;

    conn1.ping().await?;

    #[cfg(feature = "compression")]
    {
        export_import(&mut conn1, &mut conn2, Some(false)).await?;
        sqlx_exasol::__rt::sleep(std::time::Duration::from_secs(10)).await;
        export_import(&mut conn1, &mut conn2, Some(true)).await?;
        sqlx_exasol::__rt::sleep(std::time::Duration::from_secs(10)).await;
    }

    export_import(&mut conn1, &mut conn2, None).await?;

    assert_eq!(conn1.attributes().encryption_enabled(), tls_expected);
    assert_eq!(conn2.attributes().encryption_enabled(), tls_expected);
    assert_eq!(
        conn1.attributes().compression_enabled(),
        compression_expected
    );
    assert_eq!(
        conn2.attributes().compression_enabled(),
        compression_expected
    );

    Ok(())
}

#[allow(unused_variables, reason = "conditionally compiled")]
async fn export_import(
    conn1: &mut ExaConnection,
    conn2: &mut ExaConnection,
    etl_compression: Option<bool>,
) -> Result<(), BoxDynError> {
    const NUM_ROWS: usize = 1_000_000;

    conn1
        .execute("CREATE TABLE TLS_COMP_COMBO ( col VARCHAR(200) );")
        .await?;

    sqlx_exasol::query("INSERT INTO TLS_COMP_COMBO VALUES (?)")
        .bind(vec!["dummy"; NUM_ROWS])
        .execute(&mut *conn1)
        .await?;

    #[allow(unused_mut, reason = "conditionally compiled")]
    let mut export_builder = ExportBuilder::new(ExportSource::Table("TLS_COMP_COMBO"));
    #[allow(unused_mut, reason = "conditionally compiled")]
    let mut import_builder = ImportBuilder::new("TLS_COMP_COMBO");

    #[cfg(feature = "compression")]
    if let Some(compression) = etl_compression {
        export_builder.compression(compression);
        import_builder.compression(compression);
    }

    let (export_fut, readers) = export_builder.build(conn1).await?;
    let (import_fut, writers) = import_builder.build(conn2).await?;
    let transport_futs = std::iter::zip(readers, writers).map(|(r, w)| pipe(r, w));

    let (export_res, import_res, _) = try_join3(
        export_fut.map_err(From::from),
        import_fut.map_err(From::from),
        try_join_all(transport_futs),
    )
    .await?;

    assert_eq!(NUM_ROWS as u64, export_res.rows_affected(), "exported rows");
    assert_eq!(NUM_ROWS as u64, import_res.rows_affected(), "imported rows");

    let num_rows: i64 = sqlx_exasol::query_scalar("SELECT COUNT(*) FROM TLS_COMP_COMBO")
        .fetch_one(&mut *conn1)
        .await?;

    assert_eq!(num_rows, 2 * NUM_ROWS as i64, "export + import rows");

    sqlx_exasol::query("DROP TABLE TLS_COMP_COMBO;")
        .execute(&mut *conn1)
        .await?;

    Ok(())
}

async fn pipe(mut reader: ExaExport, mut writer: ExaImport) -> Result<(), BoxDynError> {
    let mut buf = vec![0; 5120].into_boxed_slice();
    let mut read = 1;

    while read > 0 {
        read = reader.read(&mut buf).await?;
        writer.write_all(&buf[..read]).await?;
    }

    writer.close().await?;
    Ok(())
}
