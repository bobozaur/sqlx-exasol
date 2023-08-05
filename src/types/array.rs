use serde::Serialize;
use sqlx_core::{
    database::Database,
    encode::{Encode, IsNull},
    types::Type,
};

use crate::{arguments::ExaBuffer, Exasol};

use super::ExaParameter;

impl<T> ExaParameter for &[T]
where
    T: Serialize,
{
    fn parameter_set_len(&self) -> Option<usize> {
        Some(self.len())
    }
}

impl<T> Type<Exasol> for &[T]
where
    T: Type<Exasol>,
{
    fn type_info() -> <Exasol as Database>::TypeInfo {
        T::type_info()
    }
}

impl<'q, T> Encode<'q, Exasol> for &[T]
where
    T: Encode<'q, Exasol> + Type<Exasol> + 'q + Serialize,
{
    fn produces(&self) -> Option<<Exasol as Database>::TypeInfo> {
        if let Some(v) = self.first() {
            v.produces()
        } else {
            Some(T::type_info())
        }
    }

    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> IsNull {
        buf.append(self);
        IsNull::No
    }

    fn size_hint(&self) -> usize {
        match self.first() {
            Some(v) => v.size_hint() * self.len(),
            None => 0,
        }
    }
}

impl<T> ExaParameter for Vec<T>
where
    T: Serialize,
{
    fn parameter_set_len(&self) -> Option<usize> {
        Some(self.len())
    }
}

impl<T> Type<Exasol> for Vec<T>
where
    T: Type<Exasol>,
{
    fn type_info() -> <Exasol as Database>::TypeInfo {
        T::type_info()
    }
}

impl<'q, T> Encode<'q, Exasol> for Vec<T>
where
    T: Encode<'q, Exasol> + Type<Exasol> + 'q + Serialize,
{
    fn produces(&self) -> Option<<Exasol as Database>::TypeInfo> {
        if let Some(v) = self.first() {
            v.produces()
        } else {
            Some(T::type_info())
        }
    }

    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> IsNull {
        buf.append(self);
        IsNull::No
    }

    fn size_hint(&self) -> usize {
        match self.first() {
            Some(v) => v.size_hint() * self.len(),
            None => 0,
        }
    }
}

impl<T> ExaParameter for Box<[T]>
where
    T: Serialize,
{
    fn parameter_set_len(&self) -> Option<usize> {
        Some(self.len())
    }
}

impl<T> Type<Exasol> for Box<[T]>
where
    T: Type<Exasol>,
{
    fn type_info() -> <Exasol as Database>::TypeInfo {
        T::type_info()
    }
}

impl<'q, T> Encode<'q, Exasol> for Box<[T]>
where
    T: Encode<'q, Exasol> + Type<Exasol> + 'q + Serialize,
{
    fn produces(&self) -> Option<<Exasol as Database>::TypeInfo> {
        if let Some(v) = self.first() {
            v.produces()
        } else {
            Some(T::type_info())
        }
    }

    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> IsNull {
        buf.append(self);
        IsNull::No
    }

    fn size_hint(&self) -> usize {
        match self.first() {
            Some(v) => v.size_hint() * self.len(),
            None => 0,
        }
    }
}
