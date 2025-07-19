use std::{marker::PhantomData, rc::Rc, sync::Arc};

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
/// cannot be implemented in a generic manner.
///
/// Note that the [`Encode`] trait requires the ability to encode by reference, thus the adapter
/// takes a reference that implements [`IntoIterator`]. But since iteration requires mutability,
/// the adaptar also requires [`Clone`]. The adapter definition should steer it towards being used
/// with cheaply clonable iterators since it expects the iteration elements to be references.
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
pub struct ExaIter<'i, I, T>
where
    I: IntoIterator<Item = &'i T> + Clone,
    T: 'i,
{
    into_iter: I,
    data_lifetime: PhantomData<&'i ()>,
}

impl<'i, 'q, I, T> ExaIter<'i, I, T>
where
    I: IntoIterator<Item = &'i T> + Clone,
    T: 'i + Type<Exasol> + Encode<'q, Exasol>,
    'i: 'q,
{
    pub fn new(into_iter: I) -> Self {
        Self {
            into_iter,
            data_lifetime: PhantomData,
        }
    }
}

impl<'i, I, T> Type<Exasol> for ExaIter<'i, I, T>
where
    I: IntoIterator<Item = &'i T> + Clone,
    T: 'i + Type<Exasol>,
{
    fn type_info() -> <Exasol as Database>::TypeInfo {
        <I as IntoIterator>::Item::type_info()
    }
}

impl<'i, 'q, I, T> Encode<'q, Exasol> for ExaIter<'i, I, T>
where
    I: IntoIterator<Item = &'i T> + Clone,
    T: 'i + Encode<'q, Exasol>,
    'i: 'q,
{
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        buf.append_iter(self.into_iter.clone())?;
        Ok(IsNull::No)
    }

    fn size_hint(&self) -> usize {
        self.into_iter
            .clone()
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
impl_for_iterator!([T; N], bounds => const N: usize);
impl_for_iterator!(Vec<T>);
impl_for_iterator!(Box<[T]>, deref => &**);
impl_for_iterator!(Rc<[T]>, deref => &**);
impl_for_iterator!(Arc<[T]>, deref => &**);
