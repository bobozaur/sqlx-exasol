use sqlx_core::{
    database::Database,
    encode::{Encode, IsNull},
    types::Type,
};

use crate::{arguments::ExaBuffer, Exasol};

impl<'q, T> Encode<'q, Exasol> for Option<T>
where
    T: Encode<'q, Exasol> + Type<Exasol> + 'q,
{
    #[inline]
    fn produces(&self) -> Option<<Exasol as Database>::TypeInfo> {
        if let Some(v) = self {
            v.produces()
        } else {
            Some(T::type_info())
        }
    }

    #[inline]
    fn encode(self, buf: &mut ExaBuffer) -> IsNull {
        if let Some(v) = self {
            v.encode(buf)
        } else {
            buf.append(());
            IsNull::Yes
        }
    }

    #[inline]
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> IsNull {
        if let Some(v) = self {
            v.encode_by_ref(buf)
        } else {
            buf.append(());
            IsNull::Yes
        }
    }

    #[inline]
    fn size_hint(&self) -> usize {
        self.as_ref().map_or(0, Encode::size_hint)
    }
}
