use std::sync::Arc;

use sqlx_core::{
    column::ColumnIndex, database::Database, ext::ustr::UStr, impl_statement_query,
    sql_str::SqlStr, statement::Statement, Either, HashMap,
};

use crate::{
    arguments::ExaArguments, column::ExaColumn, database::Exasol, type_info::ExaTypeInfo,
    SqlxError, SqlxResult,
};

/// Implementor of [`Statement`].
#[derive(Debug, Clone)]
pub struct ExaStatement {
    pub(crate) sql: SqlStr,
    pub(crate) metadata: ExaStatementMetadata,
}

#[derive(Debug, Clone)]
pub struct ExaStatementMetadata {
    pub columns: Arc<[ExaColumn]>,
    pub column_names: Arc<HashMap<UStr, usize>>,
    pub parameters: Arc<[ExaTypeInfo]>,
}

impl ExaStatementMetadata {
    pub fn new(columns: Arc<[ExaColumn]>, parameters: Arc<[ExaTypeInfo]>) -> Self {
        let column_names = columns
            .as_ref()
            .iter()
            .enumerate()
            .map(|(idx, col)| (col.name.clone(), idx))
            .collect();

        Self {
            columns,
            column_names: Arc::new(column_names),
            parameters,
        }
    }
}

impl Statement for ExaStatement {
    type Database = Exasol;

    fn into_sql(self) -> SqlStr {
        self.sql
    }

    fn sql(&self) -> &SqlStr {
        &self.sql
    }

    fn parameters(&self) -> Option<Either<&[<Self::Database as Database>::TypeInfo], usize>> {
        Some(Either::Left(&self.metadata.parameters))
    }

    fn columns(&self) -> &[<Self::Database as Database>::Column] {
        &self.metadata.columns
    }

    impl_statement_query!(ExaArguments);
}

impl ColumnIndex<ExaStatement> for &str {
    fn index(&self, statement: &ExaStatement) -> SqlxResult<usize> {
        statement
            .metadata
            .column_names
            .get(*self)
            .ok_or_else(|| SqlxError::ColumnNotFound((*self).into()))
            .copied()
    }
}
