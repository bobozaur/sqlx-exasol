use crate::column::ExaColumn;
use crate::database::Exasol;
use crate::value::ExaValueRef;

use serde_json::Value;
use sqlx_core::column::ColumnIndex;
use sqlx_core::database::{Database, HasValueRef};
use sqlx_core::row::Row;
use sqlx_core::{Error as SqlxError, HashMap};
use std::fmt::Debug;
use std::sync::Arc;

/// Struct representing a result set row.
/// This is only used internally to further deserialize it into a given Rust type.
///
/// To avoid intermediary allocations, we just index in the mutable array of columns,
/// and take out values based on the `row_offset`.
#[derive(Debug)]
pub struct ExaRow {
    column_names: Arc<HashMap<Arc<str>, usize>>,
    columns: Arc<[ExaColumn]>,
    data: Arc<[Vec<Value>]>,
    row_offset: usize,
}

impl ExaRow {
    pub fn new(
        data: Arc<[Vec<Value>]>,
        columns: Arc<[ExaColumn]>,
        column_names: Arc<HashMap<Arc<str>, usize>>,
        row_offset: usize,
    ) -> Self {
        Self {
            column_names,
            columns,
            data,
            row_offset,
        }
    }
}

impl Row for ExaRow {
    type Database = Exasol;

    fn columns(&self) -> &[<Self::Database as Database>::Column] {
        &self.columns
    }

    fn try_get_raw<I>(
        &self,
        index: I,
    ) -> Result<<Self::Database as HasValueRef<'_>>::ValueRef, SqlxError>
    where
        I: ColumnIndex<Self>,
    {
        let col_idx = index.index(self)?;
        let err_fn = || SqlxError::ColumnIndexOutOfBounds {
            index: col_idx,
            len: self.columns.len(),
        };

        let value = self
            .data
            .get(col_idx)
            .ok_or_else(err_fn)?
            .get(self.row_offset)
            .ok_or_else(|| SqlxError::RowNotFound)?;

        let type_info = &self.columns.get(col_idx).ok_or_else(err_fn)?.datatype;
        let val = ExaValueRef { value, type_info };

        Ok(val)
    }
}

impl ColumnIndex<ExaRow> for &'_ str {
    fn index(&self, container: &ExaRow) -> Result<usize, SqlxError> {
        container
            .column_names
            .get(*self)
            .copied()
            .ok_or_else(|| SqlxError::ColumnNotFound(self.to_string()))
    }
}
