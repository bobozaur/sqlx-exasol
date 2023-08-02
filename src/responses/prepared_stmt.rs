use std::sync::Arc;

use crate::column::ExaColumn;

/// Struct representing a prepared statement handle and column parameters metadata.
#[derive(Clone, Debug)]
pub struct PreparedStatement {
    pub(crate) statement_handle: u16,
    pub(crate) columns: Arc<[ExaColumn]>,
}
