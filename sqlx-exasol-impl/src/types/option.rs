use sqlx_core::{
    encode::{Encode, IsNull},
    error::BoxDynError,
    types::Type,
};

use crate::{arguments::ExaBuffer, types::ExaHasArrayType, ExaTypeInfo, Exasol};

impl<T> ExaHasArrayType for Option<T> where T: ExaHasArrayType {}

impl<T> Encode<'_, Exasol> for Option<T>
where
    for<'q> T: Encode<'q, Exasol> + Type<Exasol>,
{
    #[inline]
    fn produces(&self) -> Option<ExaTypeInfo> {
        if let Some(v) = self {
            v.produces()
        } else {
            Some(T::type_info())
        }
    }

    #[inline]
    fn encode(self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        if let Some(v) = self {
            v.encode(buf)
        } else {
            buf.append(())?;
            Ok(IsNull::Yes)
        }
    }

    #[inline]
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        if let Some(v) = self {
            v.encode_by_ref(buf)
        } else {
            buf.append(())?;
            Ok(IsNull::Yes)
        }
    }

    #[inline]
    fn size_hint(&self) -> usize {
        // We encode `null` when `None`, hence size 4.
        self.as_ref().map_or(4, Encode::size_hint)
    }
}
