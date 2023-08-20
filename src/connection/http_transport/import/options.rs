
use crate::connection::http_transport::RowSeparator;

#[derive(Clone, Debug)]
pub struct ImportOptions<'a> {
    num_writers: usize,
    compression: bool,
    encryption: bool,
    dest_table: &'a str,
    columns: Option<&'a [&'a str]>,
    comment: Option<&'a str>,
    encoding: Option<&'a str>,
    null: &'a str,
    row_separator: RowSeparator,
    column_separator: &'a str,
    column_delimiter: &'a str,
    skip: u64,
    trim: Option<Trim>,
}

impl<'a> ImportOptions<'a> {
    pub(crate) fn new(
        dest_table: &'a str,
        columns: Option<&'a [&'a str]>,
        encryption: bool,
    ) -> Self {
        Self {
            num_writers: 0,
            compression: false,
            encryption,
            dest_table,
            columns,
            comment: None,
            encoding: None,
            null: "",
            row_separator: RowSeparator::CRLF,
            column_separator: ",",
            column_delimiter: "\"",
            skip: 0,
            trim: None,
        }
    }

    /// Sets the number of writer jobs that will be started.
    /// If set to `0`, then as many as possible will be used (one per node).
    /// Providing a number bigger than the number of nodes is the same as providing `0`.
    pub fn num_writers(&mut self, num_writers: usize) -> &mut Self {
        self.num_writers = num_writers;
        self
    }

    pub fn compression(&mut self, enabled: bool) -> &mut Self {
        self.compression = enabled;
        self
    }

    pub fn comment(&mut self, comment: &'a str) -> &mut Self {
        self.comment = Some(comment);
        self
    }

    pub fn encoding(&mut self, encoding: &'a str) -> &mut Self {
        self.encoding = Some(encoding);
        self
    }

    pub fn null(&mut self, null: &'a str) -> &mut Self {
        self.null = null;
        self
    }

    pub fn row_separator(&mut self, separator: RowSeparator) -> &mut Self {
        self.row_separator = separator;
        self
    }

    pub fn column_separator(&mut self, separator: &'a str) -> &mut Self {
        self.column_separator = separator;
        self
    }

    pub fn column_delimiter(&mut self, delimiter: &'a str) -> &mut Self {
        self.column_delimiter = delimiter;
        self
    }

    pub fn skip(&mut self, num: u64) -> &mut Self {
        self.skip = num;
        self
    }

    pub fn trim(&mut self, trim: Trim) -> &mut Self {
        self.trim = Some(trim);
        self
    }
}

/// Trim options for IMPORT
#[derive(Debug, Clone, Copy)]
pub enum Trim {
    Left,
    Right,
    Both,
}

impl AsRef<str> for Trim {
    fn as_ref(&self) -> &str {
        match self {
            Self::Left => "LTRIM",
            Self::Right => "RTRIM",
            Self::Both => "TRIM",
        }
    }
}
