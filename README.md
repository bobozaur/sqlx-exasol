[![Crates.io](https://img.shields.io/crates/v/sqlx-exasol)](https://crates.io/crates/sqlx-exasol)
[![Docs.rs](https://img.shields.io/docsrs/sqlx-exasol)](https://docs.rs/sqlx-exasol/latest/sqlx_exasol/)

# sqlx-exasol

A database driver for Exasol to be used with the Rust [sqlx](https://github.com/launchbadge/sqlx) framework, based on the Exasol [Websocket API](https://github.com/exasol/websocket-api).
Inspired by [Py-Exasol](https://github.com/exasol/pyexasol) and based on the (now archived) [rust-exasol](https://github.com/bobozaur/rust-exasol) sync driver.

## Note

> The crate's version resembles the `sqlx` version it is based on so that managing dependencies is simpler.
>
> With that in mind, please favor using a fixed version of `sqlx` and `sqlx-exasol` in `Cargo.toml` to avoid issues, such as:
>
> ```toml
> sqlx = "=0.8.6"
> sqlx-exasol = "=0.8.6"
> ```

## Crate Features flags

- `etl` - enables the usage ETL jobs without TLS encryption.
- `etl_native_tls` - enables the `etl` feature and adds TLS encryption through
  `native-tls`<sup>[1](#etl_tls)</sup>
- `etl_rustls` - enables the `etl` feature and adds TLS encryption through
  `rustls`<sup>[1](#etl_tls)</sup>
- `compression` - enables compression support (for both connections and ETL jobs)
- `uuid` - enables support for the `uuid` crate
- `chrono` - enables support for the `chrono` crate types
- `rust_decimal` - enables support for the `rust_decimal` type
- `migrate` - enables the use of migrations and testing (just like in other `sqlx` drivers).

## Comparison to native sqlx drivers

Since the driver is used through `sqlx` and it implements the interfaces there, it can do all
the drivers shipped with `sqlx` do, with some caveats:

- Limitations

  - no compile-time query check support<sup>[2](#sqlx_limitations)</sup>
  - no `sqlx-cli` support<sup>[2](#sqlx_limitations)</sup>
  - no locking migrations support<sup>[3](#no_locks)</sup>
  - no column nullability checks<sup>[4](#nullable)</sup>

- Additions
  - array-like parameter binding in queries, thanks to the columnar nature of the Exasol
    database
  - performant & parallelizable ETL IMPORT/EXPORT jobs in CSV format through HTTP Transport

## Connection string

The connection string is expected to be an URL with the `exa://` scheme, e.g:
`exa://sys:exasol@localhost:8563`.

## Examples

Using the driver for regular database interactions:

```rust
use std::env;
use sqlx_exasol::*;

let pool = ExaPool::connect(&env::var("DATABASE_URL").unwrap()).await?;
let mut con = pool.acquire().await?;

sqlx::query("CREATE SCHEMA RUST_DOC_TEST")
    .execute(&mut *con)
    .await?;
```

Array-like parameter binding, also featuring the [`ExaIter`] adapter.
An important thing to note is that the parameter sets must be of equal length,
otherwise an error is thrown:

```rust
use std::{collections::HashSet, env};
use sqlx_exasol::*;

let pool = ExaPool::connect(&env::var("DATABASE_URL").unwrap()).await?;
let mut con = pool.acquire().await?;

let params1 = vec![1, 2, 3];
let params2 = HashSet::from([1, 2, 3]);

sqlx::query("INSERT INTO MY_TABLE VALUES (?, ?)")
    .bind(&params1)
    .bind(ExaIter::from(&params2))
    .execute(&mut *con)
    .await?;
```

An EXPORT - IMPORT ETL data pipe.

```rust
use std::env;
use futures_util::{
    future::{try_join, try_join3, try_join_all},
    AsyncReadExt, AsyncWriteExt, TryFutureExt,
};
use sqlx_exasol::{etl::*, *};

async fn pipe(mut reader: ExaExport, mut writer: ExaImport) -> anyhow::Result<()> {
    let mut buf = vec![0; 5120].into_boxed_slice();
    let mut read = 1;

    while read > 0 {
        // Readers return EOF when there's no more data.
        read = reader.read(&mut buf).await?;
        // Write data to Exasol
        writer.write_all(&buf[..read]).await?;
    }

    // Writes, unlike readers, MUST be closed to signal we won't send more data to Exasol
    writer.close().await?;
    Ok(())
}

let pool = ExaPool::connect(&env::var("DATABASE_URL").unwrap()).await?;
let mut con1 = pool.acquire().await?;
let mut con2 = pool.acquire().await?;

// Build EXPORT job
let (export_fut, readers) = ExportBuilder::new(ExportSource::Table("TEST_ETL"))
    .build(&mut con1)
    .await?;

// Build IMPORT job
let (import_fut, writers) = ImportBuilder::new("TEST_ETL").build(&mut con2).await?;

// Use readers and writers in some futures
let transport_futs = std::iter::zip(readers, writers).map(|(r, w)| pipe(r, w));

// Execute the EXPORT and IMPORT query futures along with the worker futures
let (export_res, import_res, _) = try_join3(
    export_fut.map_err(From::from),
    import_fut.map_err(From::from),
    try_join_all(transport_futs),
)
.await?;

assert_eq!(export_res.rows_affected(), import_res.rows_affected());
```

## License

Licensed under either of

- Apache License, Version 2.0, (LICENSE-APACHE or https://www.apache.org/licenses/LICENSE-2.0)
- MIT license (LICENSE-MIT or https://opensource.org/licenses/MIT)

at your option.

## Contributing

Contributions to this repository, unless explicitly stated otherwise, will be considered dual-licensed under MIT and Apache 2.0.
Bugs/issues encountered can be opened [here](https://github.com/bobozaur/sqlx-exasol/issues)

## Footnotes

<a name= etl_tls>1</a>: There is unfortunately no way to automagically choose a crate's feature flags based on its dependencies feature flags, so the TLS backend has
to be manually selected. While nothing prevents you from using, say `native-tls` with `sqlx` and `rustls` with Exasol ETL jobs, it might be best to avoid compiling
two different TLS backends. Therefore, consider choosing the `sqlx` and `sqlx-exasol` feature flags in a consistent manner.

<a name="sqlx_limitations">2</a>: The `sqlx` API powering the compile-time query checks and the `sqlx-cli` tool is not public. Even if it were, the drivers that are incorporated into `sqlx` are hardcoded in the part of the code that handles the compile-time driver decision logic. <br>The main problem from what I can gather is that there's no easy way of defining a plugin system in Rust at the moment, hence the hardcoding.

<a name="no_locks">3</a>: Exasol has no advisory or database locks and simple, unnested, transactions are unfortunately not enough to define a mechanism so that concurrent migrations do not collide. This does **not** pose a problem when migrations are run sequentially or do not act on the same database objects.

<a name="nullable">4</a>: Exasol does not provide the information of whether a column is nullable or not, so the driver cannot implicitly decide whether a `NULL` value can go into a certain database column or not until it actually tries.
