[![Crates.io](https://img.shields.io/crates/v/sqlx-exasol)](https://crates.io/crates/sqlx-exasol)
[![Docs.rs](https://img.shields.io/docsrs/sqlx-exasol)](https://docs.rs/sqlx-exasol/latest/sqlx_exasol/)

# sqlx-exasol

A database driver for Exasol to be used with the Rust [sqlx](https://github.com/launchbadge/sqlx) framework, based on the Exasol [Websocket API](https://github.com/exasol/websocket-api).
Inspired by [Py-Exasol](https://github.com/exasol/pyexasol) and based on the (now archived) [rust-exasol](https://github.com/bobozaur/rust-exasol) sync driver.

Based on `sqlx` version `0.9.0-alpha.1`.

## Comparison to native sqlx drivers

The driver re-exports all `sqlx` public API and implements the exposed traits. As a result, 
it can do all the drivers shipped with `sqlx` do, with some caveats:

- Limitations
  - separate CLI utility (`sqlx-exasol` instead of `sqlx`)
  - compile time query macros cannot work along the ones from `sqlx` within the same crate
  - no locking migrations support<sup>[1](#no_locks)</sup>
  - no column nullability checks<sup>[2](#nullable)</sup>

- Additions
  - array-like parameter binding in queries, thanks to the columnar nature of the Exasol database
  - performant & parallelizable ETL IMPORT/EXPORT jobs in CSV format through HTTP Transport

## Compile-time query checks

The driver now supports compile-time query validation. 

However, full functionality is implemented through path overrides and due to `sqlx` macros 
implementation details you will currently need to either add `extern crate sqlx_exasol as sqlx;` 
to the root of your crate or rename the crate import to `sqlx` in `Cargo.toml`:

```toml
sqlx = { version = "*", package = "sqlx-exasol" }
```

This implies that the compile time query macros from both `sqlx-exasol` and `sqlx`
cannot co-exist within the same crate without collisions or unexpected surprises.

See <https://github.com/launchbadge/sqlx/pull/3944> for more details.

## CLI utility

The driver uses its own CLI utility (built on the same `sqlx-cli` library):
```sh
cargo install sqlx-exasol-cli

# Usage is exactly the same as sqlx-cli
sqlx-exasol database create
sqlx-exasol database drop
sqlx-exasol migrate add <name>
sqlx-exasol migrate run
cargo sqlx-exasol prepare
```

## Connection string

The connection string is expected to be an URL with the `exa://` scheme, e.g:
`exa://sys:exasol@localhost:8563`.

## Example

```rust,no_run
use std::env;
use sqlx_exasol::{error::*, *};

let pool = ExaPool::connect(&env::var("DATABASE_URL").unwrap()).await?;
let mut con = pool.acquire().await?;

sqlx_exasol::query("CREATE SCHEMA RUST_DOC_TEST")
    .execute(&mut *con)
    .await?;
```

See the crate documentation for more details and advanced examples.

## License

Licensed under either of

- Apache License, Version 2.0, (LICENSE-APACHE or https://www.apache.org/licenses/LICENSE-2.0)
- MIT license (LICENSE-MIT or https://opensource.org/licenses/MIT)

at your option.

## Contributing

Contributions to this repository, unless explicitly stated otherwise, will be considered dual-licensed under MIT and Apache 2.0.
Bugs/issues encountered can be opened [here](https://github.com/bobozaur/sqlx-exasol/issues)

## Footnotes

<a name="no_locks">1</a>: Exasol has no advisory or database locks and simple, unnested, transactions are unfortunately not enough to define a mechanism so that concurrent migrations do not collide. This does **not** pose a problem when migrations are run sequentially or do not act on the same database objects.

<a name="nullable">2</a>: Exasol does not provide the information of whether a column is nullable or not, so the driver cannot implicitly decide whether a `NULL` value can go into a certain database column or not until it actually tries.
