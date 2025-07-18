use sqlx_core::{
    database::Database,
    encode::{Encode, IsNull},
    error::BoxDynError,
    types::Type,
};

use crate::{arguments::ExaBuffer, Exasol};

/// Adapter allowing any iterator of encodable values to be passed as a parameter set / array to
/// Exasol.
///
/// Note that the iterator must implement [`Clone`] because it's used in multiple places. Therefore,
/// prefer using iterators over references than owning variants.
///
/// ```rust
/// # use sqlx_exasol_impl as sqlx_exasol;
/// use sqlx_exasol::types::ExaIter;
///
/// // Don't do this, as the iterator gets cloned internally.
/// let vector = vec![1, 2, 3];
/// let owned_iter = ExaIter::from(vector);
///
/// // Rather, prefer using something cheaper to clone, like:
/// let vector = vec![1, 2, 3];
/// let borrowed_iter = ExaIter::from(vector.as_slice());
/// ```
#[derive(Debug)]
pub struct ExaIter<I, T>
where
    I: IntoIterator<Item = T> + Clone,
    for<'q> T: Encode<'q, Exasol> + Type<Exasol>,
{
    value: I,
}

impl<I, T> From<I> for ExaIter<I, T>
where
    I: IntoIterator<Item = T> + Clone,
    for<'q> T: Encode<'q, Exasol> + Type<Exasol>,
{
    fn from(value: I) -> Self {
        Self { value }
    }
}

impl<T, I> Type<Exasol> for ExaIter<I, T>
where
    I: IntoIterator<Item = T> + Clone,
    for<'q> T: Encode<'q, Exasol> + Type<Exasol>,
{
    fn type_info() -> <Exasol as Database>::TypeInfo {
        T::type_info()
    }
}

impl<T, I> Encode<'_, Exasol> for ExaIter<I, T>
where
    I: IntoIterator<Item = T> + Clone,
    for<'q> T: Encode<'q, Exasol> + Type<Exasol>,
{
    fn produces(&self) -> Option<<Exasol as Database>::TypeInfo> {
        self.value
            .clone()
            .into_iter()
            .next()
            .as_ref()
            .and_then(Encode::produces)
            .or_else(|| Some(T::type_info()))
    }

    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        buf.append_iter(self.value.clone())?;
        Ok(IsNull::No)
    }

    fn size_hint(&self) -> usize {
        self.value
            .clone()
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
}

impl<T> Encode<'_, Exasol> for &[T]
where
    for<'q> T: Encode<'q, Exasol> + Type<Exasol>,
{
    fn produces(&self) -> Option<<Exasol as Database>::TypeInfo> {
        self.first()
            .and_then(Encode::produces)
            .or_else(|| Some(T::type_info()))
    }

    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        buf.append_iter(self.iter())?;
        Ok(IsNull::No)
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
}

impl<T> Encode<'_, Exasol> for &mut [T]
where
    for<'q> T: Encode<'q, Exasol> + Type<Exasol>,
{
    fn produces(&self) -> Option<<Exasol as Database>::TypeInfo> {
        (&**self).produces()
    }

    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        (&**self).encode_by_ref(buf)
    }

    fn size_hint(&self) -> usize {
        (&**self).size_hint()
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
    fn produces(&self) -> Option<<Exasol as Database>::TypeInfo> {
        self.as_slice().produces()
    }

    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        self.as_slice().encode_by_ref(buf)
    }

    fn size_hint(&self) -> usize {
        self.as_slice().size_hint()
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
    fn produces(&self) -> Option<<Exasol as Database>::TypeInfo> {
        (&**self).produces()
    }

    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        (&**self).encode_by_ref(buf)
    }

    fn size_hint(&self) -> usize {
        (&**self).size_hint()
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

impl<T> Encode<'_, Exasol> for Box<[T]>
where
    for<'q> T: Encode<'q, Exasol> + Type<Exasol>,
{
    fn produces(&self) -> Option<<Exasol as Database>::TypeInfo> {
        (&**self).produces()
    }

    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        (&**self).encode_by_ref(buf)
    }

    fn size_hint(&self) -> usize {
        (&**self).size_hint()
    }
}
