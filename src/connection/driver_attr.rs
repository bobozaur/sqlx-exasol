use std::fmt::{Display, Formatter};

/// Struct holding driver related attributes
/// unrelated to the Exasol connection itself
#[derive(Debug)]
pub struct DriverAttributes {
    pub server_ip: String,
    pub port: u16,
    pub fetch_size: usize,
    pub lowercase_columns: bool,
}

impl Display for DriverAttributes {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "server_addr: {}\n\
             fetch_size:{}\n\
             lowercase_columns:{}",
            &self.server_ip, self.fetch_size, self.lowercase_columns
        )
    }
}
