# Supported types

| Rust type                 | Exasol type                                   |
| :------------------------ | :-------------------------------------------- |
| `bool`                    | `BOOLEAN`                                     |
| `i8`, `i16`, `i32`, `i64` | `DECIMAL`                                     |
| `f64`                     | `DOUBLE`                                      |
| `String`, `&str`          | `CHAR(n) ASCII/UTF8`, `VARCHAR(n) ASCII/UTF8` |
| `ExaIntervalYearToMonth`  | `INTERVAL YEAR TO MONTH`                      |
| `HashType`                | `HASHTYPE`                                    |
| `Option<T>`               | `T` (for any `T` that implements `Type`)      |

## `chrono` feature

| Rust type               | Exasol type              |
| :---------------------- | :----------------------- |
| `chrono::NaiveDate`     | `DATE`                   |
| `chrono::NaiveDateTime` | `TIMESTAMP`              |
| `chrono::TimeDelta`     | `INTERVAL DAY TO SECOND` |

## `time` feature

| Rust type                 | Exasol type              |
| :------------------------ | :----------------------- |
| `time::Date`              | `DATE`                   |
| `time::PrimitiveDateTime` | `TIMESTAMP`              |
| `time::Duration`          | `INTERVAL DAY TO SECOND` |

## `rust_decimal` feature

| Rust type               | Exasol type    |
| :---------------------- | :------------- |
| `rust_decimal::Decimal` | `DECIMAL(p,s)` |

## `bigdecimal` feature

| Rust type                | Exasol type    |
| :----------------------- | :------------- |
| `bigdecimal::BigDecimal` | `DECIMAL(p,s)` |

## `uuid` feature

| Rust type    | Exasol type |
| :----------- | :---------- |
| `uuid::Uuid` | `HASHTYPE`  |

## `geo-types` feature

| Rust type             | Exasol type |
| :-------------------- | :---------- |
| `geo_types::Geometry` | `GEOMETRY`  |

**Note:** due to a [bug in the Exasol websocket
API](httpsf://github.com/exasol/websocket-api/issues/39), `GEOMETRY` can't be used as prepared
statement bind parameters. It can, however, be used as a column in a returned result set or with
runtime checked queries.

## `json` feature

The `json` feature enables `Encode` and `Decode` implementations for `Json<T>`,
`serde_json::Value` and `&serde_json::value::RawValue`.

## Array-like parameters

Array-like types can be passed as parameters, including in compile time checked queries,
for batch parameter binding due to Exasol's columnar nature.

Supported types are [`Vec<T>`], `&T` (slices), [`[T;N]`] (arrays), iterators through the
[`ExaIter`](crate::types::ExaIter) adapter, etc.

Parameter arrays must be of equal length (runtime checked) or an error will be thrown otherwise.

Custom types that implement [`Type`] can be used in array-like types by implementing
the [`ExaHasArrayType`](crate::types::ExaHasArrayType) marker trait for them.
