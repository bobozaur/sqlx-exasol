use serde::Serialize;
use serde_json::Error as SerdeError;
use sqlx_core::{arguments::Arguments, encode::Encode, error::BoxDynError, types::Type};

use crate::{database::Exasol, error::ExaProtocolError, type_info::ExaTypeInfo};

#[derive(Debug, Default)]
pub struct ExaArguments {
    pub buf: ExaBuffer,
    pub types: Vec<ExaTypeInfo>,
}

impl<'q> Arguments<'q> for ExaArguments {
    type Database = Exasol;

    fn reserve(&mut self, additional: usize, size: usize) {
        self.types.reserve(additional);
        self.buf.buffer.reserve(size);
    }

    fn add<T>(&mut self, value: T) -> Result<(), BoxDynError>
    where
        T: 'q + Encode<'q, Self::Database> + Type<Self::Database>,
    {
        let ty = value.produces().unwrap_or_else(T::type_info);

        self.buf.start_seq();
        let _ = value.encode(&mut self.buf)?;
        self.buf.end_seq();
        self.buf.add_separator();

        self.buf.check_param_count()?;

        self.types.push(ty);

        Ok(())
    }

    fn len(&self) -> usize {
        self.types.len()
    }
}

#[derive(Debug)]
pub struct ExaBuffer {
    pub(crate) buffer: String,
    pub(crate) current_set_params: usize,
    pub(crate) first_set_params: Option<usize>,
}

impl ExaBuffer {
    /// Serializes and appends a value to this buffer.
    pub fn append<T>(&mut self, value: T) -> Result<(), SerdeError>
    where
        T: Serialize,
    {
        self.current_set_params += 1;
        // SAFETY: `serde_json` will only write valid UTF-8.
        serde_json::to_writer(unsafe { self.buffer.as_mut_vec() }, &value)
    }

    /// Serializes and appends an iterator of values to this buffer.
    pub fn append_iter<'q, I, T>(&mut self, iter: I) -> Result<(), BoxDynError>
    where
        I: IntoIterator<Item = T>,
        T: 'q + Encode<'q, Exasol> + Type<Exasol>,
    {
        let mut iter = iter.into_iter();

        if let Some(value) = iter.next() {
            let _ = value.encode(self)?;
        }

        for value in iter {
            self.add_separator();
            let _ = value.encode(self)?;
        }

        Ok(())
    }

    /// Outputs the numbers of parameter sets in the buffer.
    pub(crate) fn num_param_sets(&self) -> usize {
        self.first_set_params.unwrap_or_default()
    }

    /// Ends the main sequence serialization and returns the JSON [`String`] buffer.
    pub(crate) fn finish(mut self) -> String {
        // Pop the last `,` separator automatically added when an element is encoded and appended.
        self.buffer.pop();
        self.end_seq();
        self.buffer
    }

    /// Adds the sequence serialization start to the buffer.
    fn start_seq(&mut self) {
        self.buffer.push('[');
    }

    /// Adds the sequence serialization start to the buffer.
    fn end_seq(&mut self) {
        self.buffer.push(']');
    }

    /// Adds the sequence serialization separator to the buffer.
    fn add_separator(&mut self) {
        self.buffer.push(',');
    }

    /// Registers the number of rows we bound parameters for.
    ///
    /// The first time we add an argument, we store the number of rows
    /// we pass parameters for. This is useful for when arrays of
    /// parameters get passed for each column.
    ///
    /// All subsequent calls will check that the number of rows is the same.
    fn check_param_count(&mut self) -> Result<(), ExaProtocolError> {
        let count = self.current_set_params;

        // We must reset the count in preparation for the next parameter.
        self.current_set_params = 0;

        match self.first_set_params {
            Some(n) if n == count => (),
            Some(n) => Err(ExaProtocolError::ParameterLengthMismatch(count, n))?,
            None => self.first_set_params = Some(count),
        };

        Ok(())
    }
}

impl Default for ExaBuffer {
    fn default() -> Self {
        let mut buffer = Self {
            buffer: String::with_capacity(1),
            current_set_params: 0,
            first_set_params: None,
        };

        buffer.start_seq();
        buffer
    }
}
