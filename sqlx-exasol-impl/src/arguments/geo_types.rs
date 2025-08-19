use geo_types::{CoordNum, Geometry};
use wkt::ToWkt;

use crate::arguments::ExaBuffer;

impl ExaBuffer {
    /// Serializes a [`geo_types::Geometry`] value as a WKT string.
    pub fn append_geometry<T>(&mut self, value: &Geometry<T>) -> std::io::Result<()>
    where
        T: CoordNum + std::fmt::Display,
    {
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
}
