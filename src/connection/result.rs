use super::{Column, Connection, FetchedData, QueryResultDe, ResultSetDe, Row};
use crate::error::{DataError, DriverError, RequestError, Result};
use serde::{de::DeserializeOwned, Deserialize};
use serde_json::{json, Value};
use std::fmt::Debug;
use std::marker::PhantomData;

/// Struct representing the result of a query.
/// If the query produced a [ResultSet], it will be contained in the `result_set` field.
#[derive(Debug, Deserialize)]
#[serde(from = "QueryResultDe")]
pub struct QueryResult {
    row_count: usize,
    result_set: Option<ResultSet>,
}

impl QueryResult {
    fn new(row_count: usize, result_set: Option<ResultSet>) -> Self {
        Self {
            row_count,
            result_set,
        }
    }

    /// Returns the number of rows affected by this query.
    /// In case the query returned a result set, the row count is the number of rows
    /// in the result set.
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// Returns an optional reference of the [ResultSet] of the query.
    pub fn result_set(&self) -> Option<&ResultSet> {
        self.result_set.as_ref()
    }

    /// Returns whether the result has unretrieved rows.
    /// If the query did not result in a [ResultSet], this method returns false.
    pub fn has_rows(&self) -> bool {
        self.result_set().map(|rs| rs.is_closed()).unwrap_or(false)
    }

    /// Returns an optional mutable reference of the  [ResultSet] of the query.
    pub(crate) fn result_set_mut(&mut self) -> Option<&mut ResultSet> {
        self.result_set.as_mut()
    }

    /// Turns all columns in the result set into lower case
    /// for easier deserialization.
    pub(crate) fn lowercase_columns(&mut self, flag: bool) {
        if let Some(rs) = self.result_set.as_mut() {
            rs.lowercase_columns(flag);
        }
    }
}

impl From<QueryResultDe> for QueryResult {
    fn from(qr: QueryResultDe) -> Self {
        match qr {
            QueryResultDe::RowCount { row_count: rc } => QueryResult::new(rc, None),
            QueryResultDe::ResultSet { result_set: rs } => {
                QueryResult::new(rs.num_rows(), Some(rs))
            }
        }
    }
}

/// Struct representing a database result set.
/// You'll generally only interact with this if you need information about result set columns.
#[derive(Debug, Deserialize)]
#[serde(from = "ResultSetDe")]
pub struct ResultSet {
    num_columns: usize,
    total_rows_num: usize,
    total_rows_pos: usize,
    result_set_handle: Option<u16>,
    columns: Vec<Column>,
    is_closed: bool,
    fetched_data: FetchedData,
}

impl From<ResultSetDe> for ResultSet {
    fn from(rs: ResultSetDe) -> Self {
        let fetched_data = FetchedData {
            chunk_rows_num: rs.chunk_rows_num,
            chunk_rows_pos: 0,
            data: rs.data,
        };

        Self {
            num_columns: rs.num_columns,
            total_rows_num: rs.total_rows_num,
            total_rows_pos: 0,
            result_set_handle: rs.result_set_handle,
            columns: rs.columns,
            is_closed: false,
            fetched_data,
        }
    }
}

impl ResultSet {
    /// Returns true/false depending on whether the [ResultSet]
    /// is closed.
    #[inline]
    pub fn is_closed(&self) -> bool {
        self.is_closed
    }

    /// Returns a reference to a [Vec<Column>] of the result set columns.
    #[inline]
    pub fn columns(&self) -> &Vec<Column> {
        &self.columns
    }

    /// Returns the number of columns in the result set.
    #[inline]
    pub fn num_columns(&self) -> usize {
        self.num_columns
    }

    /// Returns the number of rows in the result set.
    #[inline]
    pub fn num_rows(&self) -> usize {
        self.total_rows_num
    }

    /// Returns the number of already retrieved rows form the result set.
    #[inline]
    pub fn position(&self) -> usize {
        self.total_rows_pos
    }

    /// Returns the result set database handle
    #[inline]
    pub fn handle(&self) -> Option<u16> {
        self.result_set_handle
    }

    /// Turns all column names to lower case for easier deserialization
    pub(crate) fn lowercase_columns(&mut self, flag: bool) {
        if flag {
            self.columns.iter_mut().for_each(|c| c.use_lowercase_name());
        }
    }
}

/// Iterator over a [ResultSet].
/// This will only be created internally, as it contains a mutable reference to the [Connection],
/// which is why [IntoIterator] is not implemented for [ResultSet].
pub struct ResultSetIter<'a, T: DeserializeOwned> {
    rs: &'a mut ResultSet,
    con: &'a mut Connection,
    err_countered: bool,
    row_type: PhantomData<T>,
}

impl<'a, T> ResultSetIter<'a, T>
where
    T: DeserializeOwned,
{
    pub fn new(rs: &'a mut ResultSet, con: &'a mut Connection) -> Self {
        Self {
            rs,
            con,
            err_countered: false,
            row_type: PhantomData,
        }
    }

    /// Validates that the row meets the required length and
    /// then deserializes it
    fn parse_row(&self, row: Vec<Value>) -> Result<T> {
        let row_len = row.len();
        let res = match row_len == self.rs.num_columns {
            true => self.deser_row(row),
            false => Err(DataError::IncorrectLength(self.rs.num_columns, row_len)),
        };

        Ok(res.map_err(DriverError::DataError)?)
    }

    /// Deserializes row to the given type
    fn deser_row(&self, row: Vec<Value>) -> std::result::Result<T, DataError> {
        T::deserialize(Row::new(row, &self.rs.columns)).map_err(DataError::TypeParseError)
    }

    /// Composes the next row from the flat data.
    fn next_row(&mut self) -> Result<T> {
        // Get a row by indexing through the 0..col_len range
        // Then the index will be chunk_rows_pos + i * chunk_rows_num
        let mut row = Vec::with_capacity(self.rs.num_columns);
        let fd = &mut self.rs.fetched_data;

        // Gather row data
        (0..self.rs.num_columns)
            .into_iter()
            .map(|i| fd.chunk_rows_pos + i * fd.chunk_rows_num)
            .map(|i| fd.data.get_mut(i).map(|v| v.take()))
            .for_each(|o| row.extend(o));

        // Parse and return row
        self.parse_row(row)
    }

    /// Closes the result set.
    /// This method gets called when a [ResultSet] is fully iterated over
    /// And that cannot happen if the [QueryResult] was already manually closed
    /// because manually closing the [QueryResult] consumes it.
    fn close(&mut self) -> Result<()> {
        self.rs.is_closed = true;
        self.rs
            .result_set_handle
            .map_or(Ok(()), |h| self.con.close_results_impl(&[h]))
    }

    /// Gets the next chunk of the result set from Exasol
    fn fetch_chunk(&mut self) -> Result<()> {
        // Check the statement handle
        self.rs
            .result_set_handle
            .ok_or_else(|| DriverError::RequestError(RequestError::MissingHandleError).into())
            .and_then(|h| {
                // Compose the payload
                let payload = json!({
                    "command": "fetch",
                    "resultSetHandle": h,
                    "startPosition": self.rs.total_rows_pos,
                    "numBytes": self.con.fetch_size()
                });

                // Fetch and store data chunk
                self.rs.fetched_data = self.con.get_resp_data(payload)?.try_into()?;
                Ok(())
            })
    }
}

/// Making [ResultSetIter] an iterator
impl<'a, T> Iterator for ResultSetIter<'a, T>
where
    T: DeserializeOwned,
{
    type Item = Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        let row = if self.rs.fetched_data.chunk_rows_pos < self.rs.fetched_data.chunk_rows_num {
            // There still are rows in this chunk
            Some(self.next_row())
        } else if self.rs.total_rows_pos >= self.rs.total_rows_num || self.err_countered {
            // All rows retrieved or an error was encountered.
            None
        } else {
            // Whole chunk retrieved. Get new one and get row.
            self.fetch_chunk()
                .map_or_else(|e| Some(Err(e)), |_| Some(self.next_row()))
        };

        // If an error was encountered when fetching the
        // next chunk, then we'll be stopping the iteration.
        //
        // We'll also NOT increment counters so as not to
        // mess up a recreation of the iterator and thus
        // further attempts to retrieve the rest of the result set.
        if let Some(Err(_)) = &row {
            self.err_countered = true;
            row
        } else {
            // Updating positional counters
            self.rs.total_rows_pos += 1;
            self.rs.fetched_data.chunk_rows_pos += 1;

            // If row is None, all rows were iterated
            // so we're closing the result set, propagating the error, if any
            row.or_else(|| self.close().map_or_else(|e| Some(Err(e)), |_| None))
        }
    }

    /// This should optimize collection as we can always know
    /// how many more rows are left in the [ResultSet]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.rs.total_rows_num - self.rs.total_rows_pos;
        (remaining, Some(remaining))
    }
}
