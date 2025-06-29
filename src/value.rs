use std::borrow::Cow;

use serde_json::Value as JsonValue;
use sqlx_core::{
    database::Database,
    value::{Value, ValueRef},
};

use crate::{database::Exasol, type_info::ExaTypeInfo};

/// Implementor of [`Value`].
#[derive(Clone, Debug)]
pub struct ExaValue {
    pub(crate) value: JsonValue,
    type_info: ExaTypeInfo,
}

/// Implementor of [`ValueRef`].
#[derive(Clone, Debug)]
pub struct ExaValueRef<'r> {
    pub(crate) value: &'r JsonValue,
    pub(crate) type_info: &'r ExaTypeInfo,
}

impl Value for ExaValue {
    type Database = Exasol;

    fn as_ref(&self) -> <Self::Database as Database>::ValueRef<'_> {
        ExaValueRef {
            value: &self.value,
            type_info: &self.type_info,
        }
    }

    fn type_info(&self) -> Cow<'_, <Self::Database as Database>::TypeInfo> {
        Cow::Borrowed(&self.type_info)
    }

    fn is_null(&self) -> bool {
        self.value.is_null()
    }
}

impl<'r> ValueRef<'r> for ExaValueRef<'r> {
    type Database = Exasol;

    fn to_owned(&self) -> <Self::Database as Database>::Value {
        ExaValue {
            value: self.value.clone(),
            type_info: *self.type_info,
        }
    }

    fn type_info(&self) -> Cow<'_, <Self::Database as Database>::TypeInfo> {
        Cow::Borrowed(self.type_info)
    }

    fn is_null(&self) -> bool {
        self.value.is_null()
    }
}
