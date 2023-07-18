use std::{borrow::Cow, collections::HashMap, sync::Arc};

use sqlx::ColumnIndex;

use crate::{column::ExaColumn, database::Exasol, type_info::ExaTypeInfo};

#[derive(Debug, Clone)]
pub struct ExaStatement<'q> {
    pub(crate) sql: Cow<'q, str>,
    pub(crate) metadata: Arc<ExaStatementMetadata>,
}

#[derive(Debug, Clone)]
pub struct ExaStatementMetadata {
    pub(crate) columns: Vec<ExaColumn>,
    pub(crate) column_names: HashMap<String, usize>,
    pub(crate) parameters: Vec<ExaTypeInfo>,
}

impl<'q> sqlx::Statement<'q> for ExaStatement<'q> {
    type Database = Exasol;

    fn to_owned(&self) -> <Self::Database as sqlx::database::HasStatement<'static>>::Statement {
        todo!()
    }

    fn sql(&self) -> &str {
        todo!()
    }

    fn parameters(
        &self,
    ) -> Option<sqlx::Either<&[<Self::Database as sqlx::Database>::TypeInfo], usize>> {
        todo!()
    }

    fn columns(&self) -> &[<Self::Database as sqlx::Database>::Column] {
        todo!()
    }

    fn query(
        &self,
    ) -> sqlx::query::Query<
        '_,
        Self::Database,
        <Self::Database as sqlx::database::HasArguments<'_>>::Arguments,
    > {
        todo!()
    }

    fn query_with<'s, A>(&'s self, arguments: A) -> sqlx::query::Query<'s, Self::Database, A>
    where
        A: sqlx::IntoArguments<'s, Self::Database>,
    {
        todo!()
    }

    fn query_as<O>(
        &self,
    ) -> sqlx::query::QueryAs<
        '_,
        Self::Database,
        O,
        <Self::Database as sqlx::database::HasArguments<'_>>::Arguments,
    >
    where
        O: for<'r> sqlx::FromRow<'r, <Self::Database as sqlx::Database>::Row>,
    {
        todo!()
    }

    fn query_as_with<'s, O, A>(
        &'s self,
        arguments: A,
    ) -> sqlx::query::QueryAs<'s, Self::Database, O, A>
    where
        O: for<'r> sqlx::FromRow<'r, <Self::Database as sqlx::Database>::Row>,
        A: sqlx::IntoArguments<'s, Self::Database>,
    {
        todo!()
    }

    fn query_scalar<O>(
        &self,
    ) -> sqlx::query::QueryScalar<
        '_,
        Self::Database,
        O,
        <Self::Database as sqlx::database::HasArguments<'_>>::Arguments,
    >
    where
        (O,): for<'r> sqlx::FromRow<'r, <Self::Database as sqlx::Database>::Row>,
    {
        todo!()
    }

    fn query_scalar_with<'s, O, A>(
        &'s self,
        arguments: A,
    ) -> sqlx::query::QueryScalar<'s, Self::Database, O, A>
    where
        (O,): for<'r> sqlx::FromRow<'r, <Self::Database as sqlx::Database>::Row>,
        A: sqlx::IntoArguments<'s, Self::Database>,
    {
        todo!()
    }
}

impl ColumnIndex<ExaStatement<'_>> for &'_ str {
    fn index(&self, container: &ExaStatement<'_>) -> Result<usize, sqlx::Error> {
        todo!()
    }
}
