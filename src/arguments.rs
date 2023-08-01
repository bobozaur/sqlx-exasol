use serde_json::Value;
use sqlx_core::{arguments::Arguments, encode::Encode, types::Type};

use crate::{database::Exasol, type_info::ExaTypeInfo};

#[derive(Debug, Default)]
pub struct ExaArguments {
    pub values: Vec<[Value; 1]>,
    pub types: Vec<ExaTypeInfo>,
}

impl<'q> Arguments<'q> for ExaArguments {
    type Database = Exasol;

    fn reserve(&mut self, additional: usize, _size: usize) {
        self.values.reserve(additional)
    }

    fn add<T>(&mut self, value: T)
    where
        T: 'q + Send + Encode<'q, Self::Database> + Type<Self::Database>,
    {
        let ty = value.produces().unwrap_or_else(T::type_info);
        self.types.push(ty);
        let _ = value.encode(&mut self.values);
    }
}
