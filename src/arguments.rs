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
        self.buf.inner.reserve(size);
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
    pub(crate) inner: Vec<u8>,
    pub(crate) params_count: usize,
    pub(crate) num_param_sets: Option<usize>,
}

impl ExaBuffer {
    /// Serializes and appends a value to this buffer.
    pub fn append<T>(&mut self, value: T) -> Result<(), SerdeError>
    where
        T: Serialize,
    {
        self.params_count += 1;
        serde_json::to_writer(&mut self.inner, &value)
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
    ///
    /// # Errors
    ///
    /// Will throw an error if a mismatch was recorded.
    pub(crate) fn num_param_sets(&self) -> usize {
        self.num_param_sets.unwrap_or_default()
    }

    /// Ends the main sequence serialization in the buffer.
    ///
    /// We're technically always guaranteed to have at least
    /// one byte in the buffer as this type is only created
    /// by [`sqlx`] through it's [`Default`] implementation.
    ///
    /// It will either overwrite the `[` set by default
    /// or a `,` separator automatically set when an element
    /// is encoded and added.
    pub(crate) fn finalize(&mut self) {
        let b = self.inner.last_mut().expect("buffer cannot be empty");
        *b = b']';
    }

    /// Adds the sequence serialization start to the buffer.
    fn start_seq(&mut self) {
        self.inner.push(b'[');
    }

    /// Adds the sequence serialization start to the buffer.
    fn end_seq(&mut self) {
        self.inner.push(b']');
    }

    /// Adds the sequence serialization separator to the buffer.
    fn add_separator(&mut self) {
        self.inner.push(b',');
    }

    /// Registers the number of rows we bound parameters for.
    ///
    /// The first time we add an argument, we store the number of rows
    /// we pass parameters for. This is useful for when arrays of
    /// parameters get passed for each column.
    ///
    /// All subsequent calls will check that the number of rows is the same.
    /// If it is not, the first mismatch is recorded so we can throw
    /// an error later (before sending data to the database).
    ///
    /// This is also due to `Encode` not throwing errors.
    fn check_param_count(&mut self) -> Result<(), ExaProtocolError> {
        let count = self.params_count;

        // We must reset the count in preparation for the next parameter.
        self.params_count = 0;

        match self.num_param_sets {
            Some(n) if n == count => (),
            Some(n) => Err(ExaProtocolError::ParameterLengthMismatch(count, n))?,
            None => self.num_param_sets = Some(count),
        };

        Ok(())
    }
}

impl Default for ExaBuffer {
    fn default() -> Self {
        let inner = Vec::with_capacity(1);
        let mut buffer = Self {
            inner,
            params_count: 0,
            num_param_sets: None,
        };

        buffer.start_seq();
        buffer
    }
}
