[![Crates.io](https://img.shields.io/crates/v/sqlx-exasol.svg)](https://crates.io/crates/sqlx-exasol)

# sqlx-exasol
A database driver for Exasol to be used with the Rust [sqlx](https://github.com/launchbadge/sqlx) framework, based on the Exasol [Websocket API](https://github.com/exasol/websocket-api).
Inspired by [Py-Exasol](https://github.com/exasol/pyexasol) and based on the (now archived) [rust-exasol](https://github.com/bobozaur/rust-exasol) sync driver.

**NOTE:** The driver is currently in it's `alpha` stage. This will change when it's seen enough usage to be declared **stable**.

## Crate Features flags:
* `etl` - enables the usage ETL jobs without TLS encryption.
* `etl_native_tls` - enables the `etl` feature and adds TLS encryption through `native-tls`
* `etl_rustls` - enables the `etl` feature and adds TLS encryption through `rustls`
* `compression` - enables compression support (for both connections and ETL jobs)
* `uuid` - enables support for the `uuid` crate
* `chrono` - enables support for the `chrono` crate types
* `rust_decimal` - enables support for the `rust_decimal` type
* `migrate` - enables the use of migrations and testing (just like in other `sqlx` drivers).

## Comparison to other drivers:
`sqlx-exasol` can be used through `sqlx` like any other driver, apart from certain limitations:
- no compile-time query check support<sup>[1](#sqlx_limitations)</sup>
- no `sqlx-cli` support<sup>[1](#sqlx_limitations)</sup>
- no locking migrations support<sup>[2](#no_locks)</sup>
- no column nullability checks<sup>[3](#nullable)</sup>
- apart from migrations, only a single query per statement is allowed (including in fixtures)<sup>[4](#single_query)</sup>

However, there are also extra features that `sqlx-exasol` comes with in constrast to the "implicit" `sqlx` drivers:
- array-like parameter binding in queries, thanks to the columnar nature of the Exasol database
- performant & parallelizable ETL IMPORT/EXPORT jobs through HTTP Transport

## License
Licensed under either of

* Apache License, Version 2.0, (LICENSE-APACHE or https://www.apache.org/licenses/LICENSE-2.0)
* MIT license (LICENSE-MIT or https://opensource.org/licenses/MIT)

at your option.

## Contributing
Contributions to this repository, unless explicitly stated otherwise, will be considered dual-licensed under MIT and Apache 2.0.  
Bugs/issues encountered can be opened [here](https://github.com/bobozaur/sqlx-exasol/issues)

<a name="sqlx_limitations">1</a>: The `sqlx` API powering the compile-time query checks and the `sqlx-cli` tool is not public. Even if it were, the drivers that are incorporated into `sqlx` are hardcoded in the part of the code that handles the compile-time driver decision logic. <br>The main problem from what I can gather is that there's no easy way of defining a plugin system in Rust at the moment, hence the hardcoding.  

<a name="no_locks">2</a>: Exasol has no advisory or database locks and simple, unnested, transactions are unfortunately not enough to define a mechanism so that concurrent migrations do not collide. This does **not** pose a problem when migrations are run sequentially or do not act on the same database objects.  

<a name="nullable">3</a>: Exasol does not provide the information of whether a column is nullable or not, so the driver cannot implicitly decide whether a `NULL` value can go into a certain database column or not until it actually tries.   

<a name="single_query">4</a>: I didn't even know this (as I never even thought of doing this), but `sqlx` allows running multiple queries in a single statement. Due to limitations with the websocket API this driver is based on, `sqlx-exasol` can only run one query at a time. <br>This is only circumvented in migrations through a somewhat limited, convoluted and possibly costly workaround that tries to split queries by `;`, which does not make it applicable for runtime queries at all.<br> 