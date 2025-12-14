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
        T::type_info()
    }
}

impl<T> Type<Exasol> for [T]
where
    T: Type<Exasol>,
{
    fn type_info() -> <Exasol as Database>::TypeInfo {
        T::type_info()
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

impl<T> Type<Exasol> for Vec<T>
where
    T: Type<Exasol>,
{
    fn type_info() -> <Exasol as Database>::TypeInfo {
        T::type_info()
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

macro_rules! forward_arr_encode_impl {
    ($for_type:ty, $($generics:tt)*) => {
        impl<T, $($generics)*> Encode<'_, Exasol> for $for_type
        where
            for<'q> T: Encode<'q, Exasol> + Type<Exasol>,
        {
            fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
                ExaIter::new(AsRef::<[T]>::as_ref(&self)).encode_by_ref(buf)
            }

            fn size_hint(&self) -> usize {
                ExaIter::new(AsRef::<[T]>::as_ref(&self)).size_hint()
            }
        }
    };
    ($for_type:ty) => {
        forward_arr_encode_impl!($for_type,);
    }
}

forward_arr_encode_impl!(&[T]);
forward_arr_encode_impl!([T; N], const N: usize);
forward_arr_encode_impl!(Vec<T>);
forward_arr_encode_impl!(Box<[T]>);
forward_arr_encode_impl!(Rc<[T]>);
forward_arr_encode_impl!(Arc<[T]>);
