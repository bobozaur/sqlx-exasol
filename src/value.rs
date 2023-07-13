use std::borrow::Cow;

use serde_json::Value;

use crate::{type_info::ExaTypeInfo, database::Exasol};

#[derive(Clone)]
pub struct ExaValue {
    pub(crate) value: Value,
    type_info: ExaTypeInfo,
}

#[derive(Clone)]
pub struct ExaValueRef<'r>(pub(crate) &'r ExaValue);

impl sqlx::Value for ExaValue {
    type Database = Exasol;

    fn as_ref(&self) -> <Self::Database as sqlx::database::HasValueRef<'_>>::ValueRef {
        ExaValueRef(self)
    }

    fn type_info(&self) -> std::borrow::Cow<'_, <Self::Database as sqlx::Database>::TypeInfo> {
        Cow::Borrowed(&self.type_info)
    }

    fn is_null(&self) -> bool {
        self.value.is_null()
    }
}

impl<'r> sqlx::ValueRef<'r> for ExaValueRef<'r> {
    type Database = Exasol;

    fn to_owned(&self) -> <Self::Database as sqlx::Database>::Value {
        self.0.to_owned()
    }

    fn type_info(&self) -> Cow<'_, <Self::Database as sqlx::Database>::TypeInfo> {
        Cow::Borrowed(&self.0.type_info)
    }

    fn is_null(&self) -> bool {
        self.0.value.is_null()
    }
}
