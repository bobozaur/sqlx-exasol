use std::borrow::Cow;

use serde_json::Value;
use sqlx_core::{
    database::{Database, HasValueRef},
    value::ValueRef,
};

use crate::{database::Exasol, type_info::ExaTypeInfo};

#[derive(Clone)]
pub struct ExaValue {
    pub(crate) value: Value,
    type_info: ExaTypeInfo,
}

#[derive(Clone)]
pub struct ExaValueRef<'r> {
    pub(crate) value: &'r Value,
    pub(crate) type_info: &'r ExaTypeInfo,
}

impl sqlx_core::value::Value for ExaValue {
    type Database = Exasol;

    fn as_ref(&self) -> <Self::Database as HasValueRef<'_>>::ValueRef {
        ExaValueRef {
            value: &self.value,
            type_info: &self.type_info,
        }
    }

    fn type_info(&self) -> std::borrow::Cow<'_, <Self::Database as Database>::TypeInfo> {
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
            type_info: self.type_info.clone(),
        }
    }

    fn type_info(&self) -> Cow<'_, <Self::Database as Database>::TypeInfo> {
        Cow::Borrowed(self.type_info)
    }

    fn is_null(&self) -> bool {
        self.value.is_null()
    }
}
