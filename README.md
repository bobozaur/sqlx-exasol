[![Crates.io](https://img.shields.io/crates/v/exasol.svg)](https://crates.io/crates/exasol)

# exasol
A synchronous database connector for Exasol written in Rust, based on the Exasol [Websocket API](https://github.com/exasol/websocket-api).  
Inspired by [Py-Exasol](https://github.com/exasol/pyexasol).

Please find the documentation [here](https://docs.rs/exasol/latest/exasol/).

Features available:
 - DSN parsing and resolving
 - Ability to use a custom TLS connector
 - Credentials & OpenID token login support
 - Single and batch query execution
 - Prepared statements
 - WSS support through native-tls and rustls
 - Compression support through flate2
 - Row deserialization into Rust types
 - Positional parameter binding
 - Named parameter binding
 - Performant single and multithreaded IMPORT/EXPORT features


## Crate Features:
* `native-tls` - (disabled by default) enables `tungstenite` WSS encryption support through native-tls
* `native-tls-vendored` - (disabled by default) enables `tungstenite` WSS encryption support through native-tls-vendored
* `rustls-tls-webpki-roots` - (disabled by default) enables `tungstenite` WSS encryption support through rustls-tls-webpki-roots
* `rustls-tls-native-roots` - (disabled by default) enables `tungstenite` WSS encryption support through rustls-tls-native-roots
* `flate2` - (disabled by default) enables compression support


## License
Licensed under either of

* Apache License, Version 2.0, (LICENSE-APACHE or https://www.apache.org/licenses/LICENSE-2.0)
* MIT license (LICENSE-MIT or https://opensource.org/licenses/MIT)

at your option.

## Contributing
Contributions to this repository, unless explicitly stated otherwise, will be considered dual-licensed under MIT and Apache 2.0.  
Bugs/issues encountered can be opened [here](https://github.com/bobozaur/rust-exasol/issues)