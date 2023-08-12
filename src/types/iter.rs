use std::marker::PhantomData;

use sqlx_core::{
    database::Database,
    encode::{Encode, IsNull},
    types::Type,
};

use crate::{arguments::ExaBuffer, Exasol};

/// Wrapper over a type `A` that can be converted to an iterator through the
/// [`AsRef`] trait.
///
/// This acts like an adapter allowing all iterators over types implementing
/// [`Encode`] to be passed as an array of parameters to Exasol.
pub struct ExaIter<A, I, T>
where
    A: AsRef<I>,
    I: IntoIterator<Item = T> + Copy,
    for<'q> T: Encode<'q, Exasol> + Type<Exasol>,
{
    value: A,
    iter_maker: PhantomData<fn() -> I>,
}

impl<A, I, T> From<A> for ExaIter<A, I, T>
where
    A: AsRef<I>,
    I: IntoIterator<Item = T> + Copy,
    for<'q> T: Encode<'q, Exasol> + Type<Exasol>,
{
    fn from(value: A) -> Self {
        Self {
            value,
            iter_maker: PhantomData,
        }
    }
}

impl<A, T, I> Type<Exasol> for ExaIter<A, I, T>
where
    A: AsRef<I>,
    I: IntoIterator<Item = T> + Copy,
    for<'q> T: Encode<'q, Exasol> + Type<Exasol>,
{
    fn type_info() -> <Exasol as Database>::TypeInfo {
        T::type_info()
    }

    fn compatible(ty: &<Exasol as Database>::TypeInfo) -> bool {
        <Self as Type<Exasol>>::type_info().compatible(ty)
    }
}

impl<A, T, I> Encode<'_, Exasol> for ExaIter<A, I, T>
where
    A: AsRef<I>,
    I: IntoIterator<Item = T> + Copy,
    for<'q> T: Encode<'q, Exasol> + Type<Exasol>,
{
    fn produces(&self) -> Option<<Exasol as Database>::TypeInfo> {
        let mut output = None;

        for value in self.value.as_ref().into_iter() {
            match (&output, value.produces()) {
                (None, Some(new)) => output = Some(new),
                (Some(old), Some(new)) if !old.compatible(&new) => output = Some(new),
                _ => (),
            }
        }

        output.or_else(|| Some(T::type_info()))
    }

    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> IsNull {
        buf.append_iter(self.value.as_ref().into_iter());
        IsNull::No
    }

    fn size_hint(&self) -> usize {
        self.value
            .as_ref()
            .into_iter()
            .fold(0, |sum, item| sum + item.size_hint())
    }
}

impl<'a, T> Type<Exasol> for &'a [T]
where
    T: Type<Exasol> + 'a,
{
    fn type_info() -> <Exasol as Database>::TypeInfo {
        T::type_info()
    }

    fn compatible(ty: &<Exasol as Database>::TypeInfo) -> bool {
        <Self as Type<Exasol>>::type_info().compatible(ty)
    }
}

impl<T> Encode<'_, Exasol> for &[T]
where
    for<'q> T: Encode<'q, Exasol> + Type<Exasol>,
{
    fn produces(&self) -> Option<<Exasol as Database>::TypeInfo> {
        let mut output = None;

        for value in self.iter() {
            match (&output, value.produces()) {
                (None, Some(new)) => output = Some(new),
                (Some(old), Some(new)) if !old.compatible(&new) => output = Some(new),
                _ => (),
            }
        }

        output.or_else(|| Some(T::type_info()))
    }

    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> IsNull {
        buf.append_iter(self.iter());
        IsNull::No
    }

    fn size_hint(&self) -> usize {
        self.iter().fold(0, |sum, item| sum + item.size_hint())
    }
}

impl<'a, T> Type<Exasol> for &'a mut [T]
where
    T: Type<Exasol> + 'a,
{
    fn type_info() -> <Exasol as Database>::TypeInfo {
        T::type_info()
    }

    fn compatible(ty: &<Exasol as Database>::TypeInfo) -> bool {
        <Self as Type<Exasol>>::type_info().compatible(ty)
    }
}

impl<T> Encode<'_, Exasol> for &mut [T]
where
    for<'q> T: Encode<'q, Exasol> + Type<Exasol>,
{
    fn produces(&self) -> Option<<Exasol as Database>::TypeInfo> {
        let mut output = None;

        for value in self.iter() {
            match (&output, value.produces()) {
                (None, Some(new)) => output = Some(new),
                (Some(old), Some(new)) if !old.compatible(&new) => output = Some(new),
                _ => (),
            }
        }

        output.or_else(|| Some(T::type_info()))
    }

    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> IsNull {
        buf.append_iter(self.iter());
        IsNull::No
    }

    fn size_hint(&self) -> usize {
        self.iter().fold(0, |sum, item| sum + item.size_hint())
    }
}

impl<T, const N: usize> Type<Exasol> for [T; N]
where
    T: Type<Exasol>,
{
    fn type_info() -> <Exasol as Database>::TypeInfo {
        T::type_info()
    }

    fn compatible(ty: &<Exasol as Database>::TypeInfo) -> bool {
        <Self as Type<Exasol>>::type_info().compatible(ty)
    }
}

impl<T, const N: usize> Encode<'_, Exasol> for [T; N]
where
    for<'q> T: Encode<'q, Exasol> + Type<Exasol>,
{
    fn produces(&self) -> Option<<Exasol as Database>::TypeInfo> {
        let mut output = None;

        for value in self.iter() {
            match (&output, value.produces()) {
                (None, Some(new)) => output = Some(new),
                (Some(old), Some(new)) if !old.compatible(&new) => output = Some(new),
                _ => (),
            }
        }

        output.or_else(|| Some(T::type_info()))
    }

    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> IsNull {
        buf.append_iter(self.iter());
        IsNull::No
    }

    fn size_hint(&self) -> usize {
        self.iter().fold(0, |sum, item| sum + item.size_hint())
    }
}

impl<T> Type<Exasol> for Vec<T>
where
    T: Type<Exasol>,
{
    fn type_info() -> <Exasol as Database>::TypeInfo {
        T::type_info()
    }

    fn compatible(ty: &<Exasol as Database>::TypeInfo) -> bool {
        <Self as Type<Exasol>>::type_info().compatible(ty)
    }
}

impl<T> Encode<'_, Exasol> for Vec<T>
where
    for<'q> T: Encode<'q, Exasol> + Type<Exasol>,
{
    fn produces(&self) -> Option<<Exasol as Database>::TypeInfo> {
        let mut output = None;

        for value in self.iter() {
            match (&output, value.produces()) {
                (None, Some(new)) => output = Some(new),
                (Some(old), Some(new)) if !old.compatible(&new) => output = Some(new),
                _ => (),
            }
        }

        output.or_else(|| Some(T::type_info()))
    }

    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> IsNull {
        buf.append_iter(self.iter());
        IsNull::No
    }

    fn size_hint(&self) -> usize {
        self.iter().fold(0, |sum, item| sum + item.size_hint())
    }
}

impl<T> Type<Exasol> for Box<[T]>
where
    T: Type<Exasol>,
{
    fn type_info() -> <Exasol as Database>::TypeInfo {
        T::type_info()
    }

    fn compatible(ty: &<Exasol as Database>::TypeInfo) -> bool {
        <Self as Type<Exasol>>::type_info().compatible(ty)
    }
}

impl<T> Encode<'_, Exasol> for Box<[T]>
where
    for<'q> T: Encode<'q, Exasol> + Type<Exasol>,
{
    fn produces(&self) -> Option<<Exasol as Database>::TypeInfo> {
        let mut output = None;

        for value in self.iter() {
            match (&output, value.produces()) {
                (None, Some(new)) => output = Some(new),
                (Some(old), Some(new)) if !old.compatible(&new) => output = Some(new),
                _ => (),
            }
        }

        output.or_else(|| Some(T::type_info()))
    }

    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> IsNull {
        buf.append_iter(self.iter());
        IsNull::No
    }

    fn size_hint(&self) -> usize {
        self.iter().fold(0, |sum, item| sum + item.size_hint())
    }
}
