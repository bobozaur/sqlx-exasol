use crate::column::ExaColumn;
use crate::database::Exasol;
use crate::error::DataError;
use serde::de::{
    DeserializeSeed, Error as SError, IntoDeserializer, MapAccess, SeqAccess, Visitor,
};
use serde::{forward_to_deserialize_any, Deserialize, Deserializer, Serialize};
use serde_json::{Error, Value};
use sqlx::ColumnIndex;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::sync::Arc;

/// Convenience alias
type DataResult<T> = std::result::Result<T, DataError>;

/// Struct representing a result set row.
/// This is only used internally to further deserialize it into a given Rust type.
///
/// To avoid intermediary allocations, we just index in the mutable array of columns,
/// and take out values based on the `row_offset`.
#[derive(Debug)]
pub struct ExaRow {
    columns: Arc<[ExaColumn]>,
    data: Arc<[Vec<Value>]>,
    row_offset: usize,
}

impl ExaRow {
    pub fn new(data: Arc<[Vec<Value>]>, columns: Arc<[ExaColumn]>, row_offset: usize) -> Self {
        Self {
            columns,
            data,
            row_offset,
        }
    }
}

impl sqlx::Row for ExaRow {
    type Database = Exasol;

    fn columns(&self) -> &[<Self::Database as sqlx::Database>::Column] {
        todo!()
    }

    fn try_get_raw<I>(
        &self,
        index: I,
    ) -> Result<<Self::Database as sqlx::database::HasValueRef<'_>>::ValueRef, sqlx::Error>
    where
        I: sqlx::ColumnIndex<Self>,
    {
        todo!()
    }
}

impl ColumnIndex<ExaRow> for &'_ str {
    fn index(&self, container: &ExaRow) -> Result<usize, sqlx::Error> {
        todo!()
    }
}

// /// Implements custom deserialization for [Row] because we own the data but borrow the columns.
// /// Furthermore, we want to be able to deserialize it to whatever the user wants,
// /// be it a sequence or map like type.
// ///
// /// It can thus be said that we implement a non-descriptive deserialization mechanism.
// /// Most of the implementation is a trimmed down version of the protocol in `serde_json`.
// impl<'de> serde::de::Deserializer<'de> for ExaRow {
//     serde::forward_to_deserialize_any! {
//         bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string
//         bytes byte_buf unit unit_struct enum option identifier
//     }

//     type Error = Error;
//     fn deserialize_any<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
//     where
//         V: serde::de::Visitor<'de>,
//     {
//         visit_map(self, visitor)
//     }

//     fn deserialize_newtype_struct<V>(
//         self,
//         name: &'static str,
//         visitor: V,
//     ) -> std::result::Result<V::Value, Self::Error>
//     where
//         V: Visitor<'de>,
//     {
//         visit_seq(self, visitor)
//     }

//     fn deserialize_seq<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
//     where
//         V: Visitor<'de>,
//     {
//         visit_seq(self, visitor)
//     }

//     fn deserialize_tuple<V>(
//         self,
//         len: usize,
//         visitor: V,
//     ) -> std::result::Result<V::Value, Self::Error>
//     where
//         V: Visitor<'de>,
//     {
//         visit_seq(self, visitor)
//     }

//     fn deserialize_tuple_struct<V>(
//         self,
//         name: &'static str,
//         len: usize,
//         visitor: V,
//     ) -> std::result::Result<V::Value, Self::Error>
//     where
//         V: Visitor<'de>,
//     {
//         visit_seq(self, visitor)
//     }

//     fn deserialize_map<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
//     where
//         V: Visitor<'de>,
//     {
//         visit_map(self, visitor)
//     }

//     fn deserialize_struct<V>(
//         self,
//         _name: &'static str,
//         _fields: &'static [&'static str],
//         visitor: V,
//     ) -> std::result::Result<V::Value, Self::Error>
//     where
//         V: Visitor<'de>,
//     {
//         visit_map(self, visitor)
//     }

//     fn deserialize_ignored_any<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
//     where
//         V: Visitor<'de>,
//     {
//         visitor.visit_unit()
//     }
// }

// struct RowDeserializer {
//     column_idx: usize,
//     row: ExaRow,
// }

// impl RowDeserializer {
//     fn new(row: ExaRow) -> Self {
//         Self { column_idx: 0, row }
//     }
// }

// impl<'de> SeqAccess<'de> for RowDeserializer {
//     type Error = Error;

//     fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
//     where
//         T: DeserializeSeed<'de>,
//     {
//         let val = self
//             .row
//             .data
//             .get_mut(self.column_idx)
//             .and_then(|v| v.get_mut(self.row.row_offset))
//             .map(|v| v.take());

//         self.column_idx += 1;

//         match val {
//             Some(value) => seed.deserialize(value).map(Some),
//             None => Err(serde::de::Error::custom("value is missing")),
//         }
//     }

//     fn size_hint(&self) -> Option<usize> {
//         Some(self.row.columns.len())
//     }
// }

// /// We keep track of the column by adding one to the index in both
// /// next key and value calls but dividing by 2.
// impl<'de> MapAccess<'de> for RowDeserializer {
//     type Error = Error;

//     fn next_key_seed<T>(&mut self, seed: T) -> std::result::Result<Option<T::Value>, Self::Error>
//     where
//         T: DeserializeSeed<'de>,
//     {
//         let idx = self.column_idx / 2;

//         let val = self.row.columns.get(idx);

//         self.column_idx += 1;

//         match val {
//             Some(col) => seed.deserialize(MapKeyDeserializer(col.name())).map(Some),
//             None => Ok(None),
//         }
//     }

//     fn next_value_seed<T>(&mut self, seed: T) -> std::result::Result<T::Value, Self::Error>
//     where
//         T: DeserializeSeed<'de>,
//     {
//         let idx = self.column_idx / 2;

//         let val = self
//             .row
//             .data
//             .get_mut(idx)
//             .and_then(|v| v.get_mut(self.row.row_offset))
//             .map(|v| v.take());

//         self.column_idx += 1;

//         match val {
//             Some(value) => seed.deserialize(value),
//             None => Err(serde::de::Error::custom("value is missing")),
//         }
//     }

//     fn size_hint(&self) -> Option<usize> {
//         Some(self.row.columns.len())
//     }
// }

// fn visit_seq<'de, V>(row: ExaRow, visitor: V) -> std::result::Result<V::Value, Error>
// where
//     V: Visitor<'de>,
// {
//     let len = row.columns.len();
//     let mut deserializer = RowDeserializer::new(row);
//     let seq = visitor.visit_seq(&mut deserializer)?;
//     let remaining = deserializer.row.data.len();
//     if remaining == 0 {
//         Ok(seq)
//     } else {
//         Err(serde::de::Error::invalid_length(
//             len,
//             &"fewer elements in seq",
//         ))
//     }
// }

// fn visit_map<'de, V>(row: ExaRow, visitor: V) -> std::result::Result<V::Value, Error>
// where
//     V: Visitor<'de>,
// {
//     let len = row.columns.len();
//     let mut deserializer = RowDeserializer::new(row);
//     let map: <V as Visitor>::Value = visitor.visit_map(&mut deserializer)?;
//     let remaining = deserializer.row.columns.len();
//     if remaining == 0 {
//         Ok(map)
//     } else {
//         Err(serde::de::Error::invalid_length(
//             len,
//             &"fewer elements in map",
//         ))
//     }
// }

// struct MapKeyDeserializer<'de>(&'de str);

// impl<'de> serde::Deserializer<'de> for MapKeyDeserializer<'de> {
//     type Error = Error;

//     forward_to_deserialize_any! {
//         bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string
//         bytes byte_buf unit unit_struct option newtype_struct enum
//         seq tuple tuple_struct map struct identifier ignored_any
//     }

//     fn deserialize_any<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
//     where
//         V: Visitor<'de>,
//     {
//         visitor.visit_borrowed_str(self.0)
//     }
// }

// /// Adapter to serialize an iterator in column major order
// /// and avoid allocation of a container that just gets serialized away.
// pub struct ColumnMajorIterAdapter<I, T>(I)
// where
//     I: IntoIterator<Item = T>,
//     T: Serialize;

// impl<I, T> ColumnMajorIterAdapter<I, T>
// where
//     I: IntoIterator<Item = T>,
//     T: Serialize,
// {
//     fn new(iter: I) -> Self {
//         Self(iter)
//     }
// }

// impl<I, T> serde::Serialize for ColumnMajorIterAdapter<I, T>
// where
//     I: IntoIterator<Item = T>,
//     T: Serialize,
// {
//     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
//     where
//         S: serde::Serializer,
//     {
//         serializer.collect_seq(self.0)
//     }
// }
