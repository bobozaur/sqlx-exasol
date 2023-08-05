use std::io::Write;

use serde::{ser::Error, Serialize};
use serde_json::value::RawValue;
use sqlx_core::{arguments::Arguments, encode::Encode, types::Type};

use crate::{
    database::Exasol, error::ExaProtocolError, type_info::ExaTypeInfo, types::ExaParameter,
};

#[derive(Debug, Default)]
pub struct ExaArguments {
    pub buf: ExaBuffer,
    pub types: Vec<ExaTypeInfo>,
}

impl<'q> Arguments<'q> for ExaArguments {
    type Database = Exasol;

    fn reserve(&mut self, additional: usize, size: usize) {
        self.buf.inner.reserve(size + additional)
    }

    fn add<T>(&mut self, value: T)
    where
        T: 'q + Send + Encode<'q, Self::Database> + Type<Self::Database>,
    {
        let ty = value.produces().unwrap_or_else(T::type_info);
        self.types.push(ty);
        self.buf.add_encodable(value);
    }
}

#[derive(Debug, Clone)]
pub struct ExaBuffer {
    inner: Vec<u8>,
    num_rows: NumRows,
}

impl ExaBuffer {
    /// Serializes and appends an [`ExaParameter`] to this buffer.
    pub fn append<T>(&mut self, value: T)
    where
        T: ExaParameter,
    {
        let num_rows = value.parameter_set_len();
        self.track_num_rows(num_rows.unwrap_or(1));

        match num_rows {
            Some(_) => serde_json::to_writer(self, &value).unwrap(),
            None => serde_json::to_writer(self, &[value]).unwrap(),
        }
    }

    /// Outputs the numbers of parameter rows in the buffer.
    ///
    /// # Errors
    ///
    /// Will throw an error if a mismatch was recorded.
    pub(crate) fn num_rows(&self) -> Result<usize, ExaProtocolError> {
        match self.num_rows {
            NumRows::NotSet => Ok(0),
            NumRows::Set(n) => Ok(n),
            NumRows::Mismatch(n, m) => Err(ExaProtocolError::ParameterLengthMismatch(n, m)),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        matches!(self.num_rows, NumRows::NotSet)
    }

    /// Ends the parameter serialization in the buffer.
    ///
    /// *MUST* be called before serializing this type.
    ///
    /// We're technically always guaranteed to have at least
    /// one byte in the buffer as this type is only created
    /// by [`sqlx`] through it's [`Default`] implementation.
    ///
    /// It will either overwrite the `[` set by default
    /// or a `,` separator automatically set when an element
    /// is encoded and added.
    pub(crate) fn end(&mut self) {
        let b = self.inner.last_mut().expect("buffer cannot be empty");
        *b = b']';
    }

    /// Handles the number of rows we bind parameters for.
    ///
    /// This *MUST* be used in every single `Encode` implementation.
    ///
    /// The first time we add an argument, we store the number of rows
    /// we pass parameters for.
    ///
    /// All subsequent calls will check that the number of rows is the same.
    /// If it is not, the first mismatch is recorded so we can throw
    /// an error later (before sending data to the database).
    ///
    /// This is also due to `Encode` not throwing errors.
    fn track_num_rows(&mut self, num_rows: usize) {
        let new_num_rows = match self.num_rows {
            NumRows::NotSet => NumRows::Set(num_rows),
            NumRows::Set(n) if n != num_rows => NumRows::Mismatch(n, num_rows),
            num_rows => num_rows,
        };

        self.num_rows = new_num_rows;
    }

    /// Encodes the value and adds it to the buffer,
    /// also appending the separator after it.
    fn add_encodable<'q, T>(&mut self, value: T)
    where
        T: 'q + Send + Encode<'q, Exasol> + Type<Exasol>,
    {
        let _ = value.encode(self);
        self.inner.push(b',');
    }
}

impl Default for ExaBuffer {
    fn default() -> Self {
        Self {
            inner: vec![b'['],
            num_rows: NumRows::NotSet,
        }
    }
}

impl Write for ExaBuffer {
    fn write(&mut self, buf: &[u8]) -> futures_io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> futures_io::Result<()> {
        self.inner.flush()
    }
}

impl Serialize for ExaBuffer {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let raw_value: &RawValue = serde_json::from_slice(&self.inner).map_err(Error::custom)?;
        raw_value.serialize(serializer)
    }
}

#[derive(Clone, Copy, Debug)]
pub enum NumRows {
    NotSet,
    Set(usize),
    Mismatch(usize, usize),
}
