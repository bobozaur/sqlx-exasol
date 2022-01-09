#[cfg(feature = "flate2")]
use flate2::{write::GzEncoder, Compression};
use std::io::Write;

/// Wrapper that will count the bytes written in the buffer
/// to also pass them to Exasol when sending a data chunk.
/// The CSV writer will take care of buffering so we don't have to.
pub struct ExaRowWriter<W: Write> {
    stream: W,
    compression: bool,
}

impl<W> ExaRowWriter<W>
where
    W: Write,
{
    pub fn new(stream: W, compression: bool) -> Self {
        Self {
            stream,
            compression,
        }
    }

    /// Writes the data chunk to the internal buffer, passing it through
    /// the compressor if needed.
    /// If compression is enabled, it's very important that the number of input bytes
    /// written to the compressor are returned, not the output bytes after compression.
    #[inline]
    #[allow(unreachable_code)]
    fn prepare_chunk(&mut self, buf: Vec<u8>) -> std::io::Result<Vec<u8>> {
        if self.compression {
            #[cfg(feature = "flate2")]
            return match buf.is_empty() {
                true => Ok(buf),
                false => {
                    let mut enc = GzEncoder::new(Vec::new(), Compression::default());
                    enc.write_all(buf.as_slice()).and(enc.finish())
                }
            };

            panic!("flate2 feature must be enabled to use compression");
        } else {
            Ok(buf)
        }
    }

    pub fn write_chunk(&mut self, buf: Vec<u8>) -> std::io::Result<()> {
        let buf = self.prepare_chunk(buf)?;

        if !buf.is_empty() {
            let len = format!("{:X}", buf.len());
            self.stream.write_all(len.as_bytes())?;
            self.stream.write_all(b"\r\n")?;
            self.stream.write_all(buf.as_slice())?;
            self.stream.write_all(b"\r\n")?;
            self.stream.flush()?;
        }
        Ok(())
    }
}
