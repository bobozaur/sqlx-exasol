mod bool;
#[cfg(feature = "chrono")]
mod chrono;
mod float;
mod int;
#[cfg(feature = "rust_decimal")]
mod rust_decimal;
mod str;
mod uint;
#[cfg(feature = "uuid")]
mod uuid;

macro_rules! impl_encode_decode {
    ($ty:ty) => {
        impl sqlx_core::encode::Encode<'_, $crate::database::Exasol> for $ty {
            fn encode_by_ref(
                &self,
                buf: &mut Vec<[serde_json::Value; 1]>,
            ) -> sqlx_core::encode::IsNull {
                buf.push([serde_json::json!(self)]);
                sqlx_core::encode::IsNull::No
            }
        }

        impl sqlx_core::decode::Decode<'_, crate::database::Exasol> for $ty {
            fn decode(
                value: $crate::value::ExaValueRef<'_>,
            ) -> Result<Self, sqlx_core::error::BoxDynError> {
                <Self as serde::Deserialize>::deserialize(value.value).map_err(From::from)
            }
        }
    };
}

pub(crate) use impl_encode_decode;
