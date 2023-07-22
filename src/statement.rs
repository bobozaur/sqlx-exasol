use std::sync::Arc;
use std::{borrow::Cow, collections::HashMap};

use either::Either;
use sqlx_core::column::ColumnIndex;
use sqlx_core::database::Database;
use sqlx_core::database::HasStatement;
use sqlx_core::impl_statement_query;
use sqlx_core::statement::Statement;
use sqlx_core::Error as SqlxError;

use crate::{arguments::ExaArguments, column::ExaColumn, database::Exasol, type_info::ExaTypeInfo};

#[derive(Debug, Clone)]
pub struct ExaStatement<'q> {
    pub(crate) sql: Cow<'q, str>,
    pub(crate) metadata: ExaStatementMetadata,
}

#[derive(Debug, Clone)]
pub struct ExaStatementMetadata {
    pub(crate) columns: Arc<[ExaColumn]>,
    pub(crate) column_names: HashMap<Arc<str>, usize>,
    pub(crate) parameters: Vec<ExaTypeInfo>,
}

impl ExaStatementMetadata {
    pub fn new(columns: Arc<[ExaColumn]>) -> Self {
        let mut column_names = HashMap::with_capacity(columns.len());
        let mut parameters = Vec::with_capacity(columns.len());

        for ExaColumn { name, datatype, .. } in columns.as_ref() {
            column_names.insert(name.to_owned(), parameters.len());
            parameters.push(datatype.clone());
        }

        Self {
            columns,
            column_names,
            parameters,
        }
    }
}

impl<'q> Statement<'q> for ExaStatement<'q> {
    type Database = Exasol;

    fn to_owned(&self) -> <Self::Database as HasStatement<'static>>::Statement {
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
    fn index(&self, statement: &ExaStatement<'_>) -> Result<usize, SqlxError> {
        statement
            .metadata
            .column_names
            .get(*self)
            .ok_or_else(|| SqlxError::ColumnNotFound((*self).into()))
            .map(|v| *v)
    }
}
