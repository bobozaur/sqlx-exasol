use std::{borrow::Cow, sync::Arc};

use sqlx_core::{
    column::ColumnIndex, database::Database, ext::ustr::UStr, impl_statement_query,
    statement::Statement, Either, HashMap,
};

use crate::{
    arguments::ExaArguments, column::ExaColumn, database::Exasol, type_info::ExaTypeInfo,
    SqlxError, SqlxResult,
};

/// Implementor of [`Statement`].
#[derive(Debug, Clone)]
pub struct ExaStatement<'q> {
    pub(crate) sql: Cow<'q, str>,
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

impl<'q> Statement<'q> for ExaStatement<'q> {
    type Database = Exasol;

    fn to_owned(&self) -> <Self::Database as Database>::Statement<'static> {
        ExaStatement {
            sql: Cow::Owned(self.sql.clone().into_owned()),
            metadata: self.metadata.clone(),
        }
    }

    fn sql(&self) -> &str {
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

impl ColumnIndex<ExaStatement<'_>> for &'_ str {
    fn index(&self, statement: &ExaStatement<'_>) -> SqlxResult<usize> {
        statement
            .metadata
            .column_names
            .get(*self)
            .ok_or_else(|| SqlxError::ColumnNotFound((*self).into()))
            .copied()
    }
}
