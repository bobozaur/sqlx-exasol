use std::{rc::Rc, sync::Arc};

use sqlx_core::{
    database::Database,
    encode::{Encode, IsNull},
    error::BoxDynError,
    types::Type,
};

use crate::{arguments::ExaBuffer, Exasol};

/// Adapter allowing any iterator of encodable values to be passed as a parameter array for a column
/// to Exasol in a single query invocation. The adapter is needed because [`Encode`] is still a
/// foreign trait and thus cannot be implemented in a generic manner.
///
/// Note that the [`Encode`] trait requires the ability to encode by reference, thus the adapter
/// takes a reference that must implement [`IntoIterator`].
///
/// ```rust
/// # use sqlx_exasol_impl as sqlx_exasol;
/// use sqlx_exasol::types::ExaIter;
///
/// let vector = vec![1, 2, 3];
/// let borrowed_iter = ExaIter::new(vector.as_slice());
/// ```
#[derive(Debug)]
#[repr(transparent)]
pub struct ExaIter<'i, I>(&'i I)
where
    I: ?Sized,
    &'i I: IntoIterator,
    <&'i I as IntoIterator>::Item: for<'q> Encode<'q, Exasol> + Type<Exasol>;

impl<'i, I> ExaIter<'i, I>
where
    I: ?Sized,
    &'i I: IntoIterator,
    <&'i I as IntoIterator>::Item: for<'q> Encode<'q, Exasol> + Type<Exasol>,
{
    pub fn new(into_iter: &'i I) -> Self {
        Self(into_iter)
    }
}

impl<'i, I> Type<Exasol> for ExaIter<'i, I>
where
    I: ?Sized,
    &'i I: IntoIterator,
    <&'i I as IntoIterator>::Item: for<'q> Encode<'q, Exasol> + Type<Exasol>,
{
    fn type_info() -> <Exasol as Database>::TypeInfo {
        <&'i I as IntoIterator>::Item::type_info()
    }
}

impl<'i, I> Encode<'_, Exasol> for ExaIter<'i, I>
where
    I: ?Sized,
    &'i I: IntoIterator,
    <&'i I as IntoIterator>::Item: for<'q> Encode<'q, Exasol> + Type<Exasol>,
{
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        buf.append_iter(self.0)?;
        Ok(IsNull::No)
    }

    fn size_hint(&self) -> usize {
        self.0
            .into_iter()
            .fold(0, |sum, item| sum + item.size_hint())
    }
}

macro_rules! impl_for_iterator {
    ($ty:ty, deref => { $($deref:tt)* }, bounds => $($bounds:tt)*) => {
        impl<T, $($bounds)* > Type<Exasol> for $ty
        where
        T: Type<Exasol>,
        {
            fn type_info() -> <Exasol as Database>::TypeInfo {
                T::type_info()
            }
        }

        impl<T, $($bounds)* > Encode<'_, Exasol> for $ty
        where
        for<'q> T: Encode<'q, Exasol> + Type<Exasol>,
        {
            fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
                ExaIter::new($($deref)* self).encode_by_ref(buf)
            }

            fn size_hint(&self) -> usize {
                ExaIter::new($($deref)* self).size_hint()
            }
        }
    };
    ($ty:ty, bounds => $($bounds:tt)*) => {
        impl_for_iterator!($ty, deref => {}, bounds => $($bounds)*);
    };
    ($ty:ty, deref => $($deref:tt)*) => {
        impl_for_iterator!($ty, deref => { $($deref)* }, bounds => );
    };
    ($ty:ty) => {
        impl_for_iterator!($ty, deref => {}, bounds => );
    };
}

impl_for_iterator!(&[T], deref => *);
impl_for_iterator!(&mut [T], deref => *);
impl_for_iterator!([T; N], bounds => const N: usize);
impl_for_iterator!(Vec<T>);
impl_for_iterator!(Box<[T]>, deref => &**);
impl_for_iterator!(Rc<[T]>, deref => &**);
impl_for_iterator!(Arc<[T]>, deref => &**);
