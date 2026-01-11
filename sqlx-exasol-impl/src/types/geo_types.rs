use std::{fmt::Display, str::FromStr};

use geo_types::CoordNum;
pub use geo_types::{
    Geometry, GeometryCollection, Line, LineString, MultiLineString, MultiPoint, MultiPolygon,
    Point, Polygon, Rect, Triangle,
};
use sqlx_core::{
    decode::Decode,
    encode::{Encode, IsNull},
    error::BoxDynError,
    types::Type,
};

use crate::{
    arguments::ExaBuffer,
    database::Exasol,
    type_info::{ExaDataType, ExaTypeInfo},
    types::ExaHasArrayType,
    value::ExaValueRef,
};

impl<T> Type<Exasol> for Geometry<T>
where
    T: CoordNum,
{
    fn type_info() -> ExaTypeInfo {
        ExaDataType::Geometry { srid: 0 }.into()
    }
}

impl<T> ExaHasArrayType for Geometry<T> where T: CoordNum {}

impl<T> Encode<'_, Exasol> for Geometry<T>
where
    T: CoordNum + Display,
{
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        buf.append_geometry(self)?;
        Ok(IsNull::No)
    }
}

impl<T> Decode<'_, Exasol> for Geometry<T>
where
    T: CoordNum + FromStr + Default,
{
    fn decode(value: ExaValueRef<'_>) -> Result<Self, BoxDynError> {
        wkt::deserialize::deserialize_wkt(value.value).map_err(From::from)
    }
}
