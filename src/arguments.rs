use serde::Serialize;
use serde_json::Value;
use sqlx_core::{arguments::Arguments, encode::Encode, types::Type};

use crate::database::Exasol;

#[derive(Debug, Default, Clone, Serialize)]
#[serde(transparent)]
pub struct ExaArguments(pub Vec<Value>);

impl<'q> Arguments<'q> for ExaArguments {
    type Database = Exasol;

    fn reserve(&mut self, additional: usize, _size: usize) {
        self.0.reserve(additional)
    }

    fn add<T>(&mut self, value: T)
    where
        T: 'q + Send + Encode<'q, Self::Database> + Type<Self::Database>,
    {
        let _ = value.encode(&mut self.0);
    }
}
