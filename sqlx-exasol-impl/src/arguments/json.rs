use serde::Serialize;
use serde_json::{ser::CharEscape, Error as JsonError, Serializer};

use crate::arguments::ExaBuffer;

impl ExaBuffer {
    /// Serializes a JSON value as a string containing JSON.
    pub fn append_json<T>(&mut self, value: T) -> Result<(), JsonError>
    where
        T: Serialize,
    {
        self.col_params_counter += 1;

        // SAFETY: `serde_json` will only write valid UTF-8.
        let writer = unsafe { self.buffer.as_mut_vec() };

        // Open the string containing JSON
        writer.push(b'"');

        // Serialize
        let mut serializer = Serializer::with_formatter(&mut *writer, ExaJsonStrFormatter);
        let res = value.serialize(&mut serializer);

        // Close string containing JSON
        writer.push(b'"');

        res
    }
}

/// Used to create strings containing JSON by double escaping special characters in a single
/// serialization pass.
struct ExaJsonStrFormatter;

impl serde_json::ser::Formatter for ExaJsonStrFormatter {
    /// Escapes the string beginning
    fn begin_string<W>(&mut self, writer: &mut W) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        writer.write_all(br#"\""#)
    }

    /// Escapes the string end
    fn end_string<W>(&mut self, writer: &mut W) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        writer.write_all(br#"\""#)
    }

    /// Escapes the special character and its inherent escape.
    fn write_char_escape<W>(
        &mut self,
        writer: &mut W,
        char_escape: CharEscape,
    ) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        #[allow(clippy::needless_raw_string_hashes, reason = "false positive")]
        let s: &[u8] = match char_escape {
            CharEscape::Quote => br#"\\\""#,
            CharEscape::ReverseSolidus => br#"\\\\"#,
            CharEscape::Solidus => br#"\\/"#,
            CharEscape::Backspace => br#"\\b"#,
            CharEscape::FormFeed => br#"\\f"#,
            CharEscape::LineFeed => br#"\\n"#,
            CharEscape::CarriageReturn => br#"\\r"#,
            CharEscape::Tab => br#"\\t"#,
            CharEscape::AsciiControl(byte) => {
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
mod tests {
    use serde_json::json;

    use super::*;

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
