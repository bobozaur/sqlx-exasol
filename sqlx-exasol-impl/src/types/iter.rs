use std::marker::PhantomData;

use sqlx_core::{
    database::Database,
    encode::{Encode, IsNull},
    error::BoxDynError,
    types::Type,
};

use crate::{arguments::ExaBuffer, Exasol};

/// Adapter allowing any iterator of encodable values to be treated and passed as a one dimensional
/// parameter array for a column to Exasol in a single query invocation. Multi dimensional arrays
/// are not supported. The adapter is needed because [`Encode`] is still a foreign trait and thus
/// cannot be implemented in a generic manner to all types implementing [`IntoIterator`].
///
/// Note that the [`Encode`] trait requires the ability to encode by reference, thus the adapter
/// takes a type that implements [`IntoIterator`]. But since iteration requires mutability,
/// the adaptar also requires [`Clone`]. The adapter definition should steer it towards being used
/// with cheaply clonable iterators since it expects the iteration elements to be references.
/// However, care should still be taken so as not to clone expensive [`IntoIterator`] types.
///
/// ```rust
/// # use sqlx_exasol_impl as sqlx_exasol;
/// use sqlx_exasol::types::ExaIter;
///
/// let vector = vec![1, 2, 3];
/// let borrowed_iter = ExaIter::new(vector.iter().filter(|v| **v % 2 == 0));
/// ```
#[derive(Debug)]
#[repr(transparent)]
pub struct ExaIter<I, T> {
    into_iter: I,
    data_lifetime: PhantomData<fn() -> T>,
}

impl<I, T> ExaIter<I, T>
where
    I: IntoIterator<Item = T> + Clone,
    T: for<'q> Encode<'q, Exasol> + Type<Exasol> + Copy,
{
    pub fn new(into_iter: I) -> Self {
        Self {
            into_iter,
            data_lifetime: PhantomData,
        }
    }
}

impl<I, T> Type<Exasol> for ExaIter<I, T>
where
    I: IntoIterator<Item = T> + Clone,
    T: Type<Exasol> + Copy,
{
    fn type_info() -> <Exasol as Database>::TypeInfo {
        <I as IntoIterator>::Item::type_info()
    }
}

impl<I, T> Encode<'_, Exasol> for ExaIter<I, T>
where
    I: IntoIterator<Item = T> + Clone,
    T: for<'q> Encode<'q, Exasol> + Copy,
{
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        buf.append_iter(self.into_iter.clone())?;
        Ok(IsNull::No)
    }

    fn size_hint(&self) -> usize {
        // Brackets [] + items size
        2 + self
            .into_iter
            .clone()
            .into_iter()
            .fold(0, |sum, item| sum + item.size_hint())
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

impl<T> Encode<'_, Exasol> for &[T]
where
    for<'q> T: Encode<'q, Exasol> + Type<Exasol>,
{
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        ExaIter::new(*self).encode_by_ref(buf)
    }

    fn size_hint(&self) -> usize {
        ExaIter::new(*self).size_hint()
    }
}

impl<T, const N: usize> Type<Exasol> for [T; N]
where
    T: Type<Exasol>,
{
    fn type_info() -> <Exasol as Database>::TypeInfo {
        T::type_info()
    }
}

impl<T, const N: usize> Encode<'_, Exasol> for [T; N]
where
    for<'q> T: Encode<'q, Exasol> + Type<Exasol>,
{
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        ExaIter::new(self.as_slice()).encode_by_ref(buf)
    }

    fn size_hint(&self) -> usize {
        ExaIter::new(self.as_slice()).size_hint()
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

impl<T> Encode<'_, Exasol> for Vec<T>
where
    for<'q> T: Encode<'q, Exasol> + Type<Exasol>,
{
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        ExaIter::new(self.as_slice()).encode_by_ref(buf)
    }

    fn size_hint(&self) -> usize {
        ExaIter::new(self.as_slice()).size_hint()
    }
}
