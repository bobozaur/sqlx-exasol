#[cfg(feature = "geo-types")]
use geo_types::Geometry;
use serde::Serialize;
use serde_json::Error as SerdeError;
use sqlx_core::{arguments::Arguments, encode::Encode, error::BoxDynError, types::Type};

use crate::{database::Exasol, error::ExaProtocolError, type_info::ExaTypeInfo};

/// Implementor of [`Arguments`].
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

/// Prepared statement parameters serialization buffer.
#[derive(Debug)]
pub struct ExaBuffer {
    /// JSON buffer where we serialize data.
    ///
    /// Storing this as a [`String`] allows for zero cost serialization of the buffer in the
    /// containing types.
    pub(crate) buffer: String,
    /// Counter for the number of parameters we are serializing for a column.
    ///
    /// While typically `1`, the columnar nature of Exasol allows us to pass parameter arrays for
    /// multiple rows. However, we must ensure that all parameter columns have the same number
    /// of rows.
    pub(crate) col_params_counter: usize,
    /// Storage for the first final value of `col_params_counter`.
    ///
    /// All subsequent columns are expected to have the same amount of rows.
    pub(crate) first_col_params_num: Option<usize>,
}

impl ExaBuffer {
    /// Serializes and appends a value to this buffer.
    pub fn append<T>(&mut self, value: T) -> Result<(), SerdeError>
    where
        T: Serialize,
    {
        self.col_params_counter += 1;
        // SAFETY: `serde_json` will only write valid UTF-8.
        serde_json::to_writer(unsafe { self.buffer.as_mut_vec() }, &value)
    }

    /// Serializes a JSON value as a string containing JSON.
    #[cfg(feature = "json")]
    pub fn append_json<T>(&mut self, value: T) -> Result<(), SerdeError>
    where
        T: Serialize,
    {
        self.col_params_counter += 1;

        // SAFETY: `serde_json` will only write valid UTF-8.
        let writer = unsafe { self.buffer.as_mut_vec() };

        // Open the string containing JSON
        writer.push(b'"');

        // Serialize
        let mut serializer =
            serde_json::Serializer::with_formatter(&mut *writer, ExaJsonStrFormatter);
        let res = value.serialize(&mut serializer);

        // Close string containing JSON
        writer.push(b'"');

        res
    }

    /// Serializes a [`geo_types::Geometry`] value as a WKT string.
    #[cfg(feature = "geo-types")]
    pub fn append_geometry<T>(&mut self, value: &Geometry<T>) -> std::io::Result<()>
    where
        T: geo_types::CoordNum + std::fmt::Display,
    {
        use wkt::ToWkt;

        self.col_params_counter += 1;

        // SAFETY: `serde_json` will only write valid UTF-8.
        let writer = unsafe { self.buffer.as_mut_vec() };

        // Open geometry string
        writer.push(b'"');

        // Serialize geometry data
        let res = value.write_wkt(&mut *writer);

        // Close geometry string
        writer.push(b'"');

        res
    }

    /// Serializes and appends an iterator of values to this buffer.
    pub fn append_iter<'q, I, T>(&mut self, iter: I) -> Result<(), BoxDynError>
    where
        I: IntoIterator<Item = T>,
        T: 'q + Encode<'q, Exasol>,
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
        self.first_col_params_num.unwrap_or_default()
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
    /// The first time we add an argument, we store the number of rows we pass parameters for. This
    /// is useful for when arrays of parameters get passed for each column.
    ///
    /// All subsequent calls will check that the number of rows is the same.
    fn check_param_count(&mut self) -> Result<(), ExaProtocolError> {
        let count = self.col_params_counter;

        // We must reset the count in preparation for the next parameter.
        self.col_params_counter = 0;

        match self.first_col_params_num {
            Some(n) if n == count => (),
            Some(n) => Err(ExaProtocolError::ParameterLengthMismatch(count, n))?,
            None => self.first_col_params_num = Some(count),
        }

        Ok(())
    }
}

impl Default for ExaBuffer {
    fn default() -> Self {
        let mut buffer = Self {
            buffer: String::with_capacity(1),
            col_params_counter: 0,
            first_col_params_num: None,
        };

        buffer.start_seq();
        buffer
    }
}

/// Used to create strings containing
#[cfg(feature = "json")]
struct ExaJsonStrFormatter;

#[cfg(feature = "json")]
impl serde_json::ser::Formatter for ExaJsonStrFormatter {
    fn begin_string<W>(&mut self, writer: &mut W) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        writer.write_all(br#"\""#)
    }

    fn end_string<W>(&mut self, writer: &mut W) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        writer.write_all(br#"\""#)
    }

    fn write_char_escape<W>(
        &mut self,
        writer: &mut W,
        char_escape: serde_json::ser::CharEscape,
    ) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        #[allow(
            clippy::enum_glob_use,
            reason = "plays better with conditional compilation"
        )]
        use serde_json::ser::CharEscape::*;

        #[allow(clippy::needless_raw_string_hashes, reason = "false positive")]
        let s: &[u8] = match char_escape {
            Quote => br#"\\\""#,
            ReverseSolidus => br#"\\\\"#,
            Solidus => br#"\\/"#,
            Backspace => br#"\\b"#,
            FormFeed => br#"\\f"#,
            LineFeed => br#"\\n"#,
            CarriageReturn => br#"\\r"#,
            Tab => br#"\\t"#,
            AsciiControl(byte) => {
                static HEX_DIGITS: [u8; 16] = *b"0123456789abcdef";
                let bytes = &[
                    b'\\',
                    b'\\',
                    b'u',
                    b'0',
                    b'0',
                    HEX_DIGITS[(byte >> 4) as usize],
                    HEX_DIGITS[(byte & 0xF) as usize],
                ];
                return writer.write_all(bytes);
            }
        };

        writer.write_all(s)
    }
}

#[cfg(test)]
#[cfg(feature = "json")]
mod tests {
    use serde::Serialize;
    use serde_json::{json, Serializer};

    use crate::arguments::ExaJsonStrFormatter;

    #[test]
    fn test_json_string() {
        let mut string = String::new();
        for i in 0..15u8 {
            string.push(' ');
            string.push(i as char);
        }

        let res = json!({
            "field1": 1,
            "field2": string
        });

        let mut provided = String::new();

        let writer = unsafe { provided.as_mut_vec() };
        writer.push(b'"');
        let mut serializer = Serializer::with_formatter(&mut *writer, ExaJsonStrFormatter);
        res.serialize(&mut serializer).unwrap();
        writer.push(b'"');

        let expected = serde_json::to_string(&res)
            .and_then(|s| serde_json::to_string(&s))
            .unwrap();

        assert_eq!(expected, provided);
    }
}
