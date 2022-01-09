use crate::con_opts::ProtocolVersion;
use crate::connection::result::QueryResult;
use crate::error::{DriverError, Error, Result};
use crate::ResultSet;
use serde::de::{DeserializeSeed, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fmt;
use std::fmt::{Debug, Display, Formatter};
use std::iter::zip;

/// Generic response received from the Exasol server
/// This is the first deserialization step
/// Used to determine whether the message
/// is a proper response, or an error
///
/// We're forced to use internal tagging as
/// ok/error responses have different adjacent fields
#[allow(non_snake_case)]
#[derive(Debug, Deserialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum Response {
    #[serde(rename_all = "camelCase")]
    Ok {
        response_data: Option<ResponseData>,
        attributes: Option<Attributes>,
    },
    Error {
        exception: ExaError,
    },
}

impl TryFrom<Response> for (Option<ResponseData>, Option<Attributes>) {
    type Error = Error;

    #[inline]
    fn try_from(resp: Response) -> Result<Self> {
        match resp {
            Response::Ok {
                response_data: data,
                attributes: attr,
            } => Ok((data, attr)),
            Response::Error { exception: e } => Err(Error::ExasolError(e)),
        }
    }
}

/// This is the `responseData` field of the JSON response.
/// Because all `ok` responses are returned through this
/// with no specific identifier between them
/// we have to use untagged deserialization.
///
/// As a result, the order of the enum variants matters,
/// as deserialization has to be non-overlapping yet exhaustive.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum ResponseData {
    PreparedStatement(PreparedStatement),
    Results(Results),
    FetchedData(FetchedData),
    PublicKey(PublicKey),
    LoginInfo(LoginInfo),
    Hosts(Hosts),
    Attributes(Attributes),
}

impl TryFrom<ResponseData> for Vec<QueryResult> {
    type Error = Error;

    fn try_from(value: ResponseData) -> std::result::Result<Self, Self::Error> {
        match value {
            ResponseData::Results(res) => Ok(res.results),
            _ => Err(DriverError::ResponseMismatch("query results").into()),
        }
    }
}

impl TryFrom<ResponseData> for PreparedStatement {
    type Error = Error;

    fn try_from(value: ResponseData) -> std::result::Result<Self, Self::Error> {
        match value {
            ResponseData::PreparedStatement(p) => Ok(p),
            _ => Err(DriverError::ResponseMismatch("prepared statement").into()),
        }
    }
}

impl TryFrom<ResponseData> for FetchedData {
    type Error = Error;

    fn try_from(value: ResponseData) -> std::result::Result<Self, Self::Error> {
        match value {
            ResponseData::FetchedData(d) => Ok(d),
            _ => Err(DriverError::ResponseMismatch("data chunk").into()),
        }
    }
}

impl TryFrom<ResponseData> for Vec<String> {
    type Error = Error;

    fn try_from(value: ResponseData) -> std::result::Result<Self, Self::Error> {
        match value {
            ResponseData::Hosts(h) => Ok(h.nodes),
            _ => Err(DriverError::ResponseMismatch("hosts").into()),
        }
    }
}

impl TryFrom<ResponseData> for String {
    type Error = Error;

    fn try_from(value: ResponseData) -> std::result::Result<Self, Self::Error> {
        match value {
            ResponseData::PublicKey(p) => Ok(p.into()),
            _ => Err(DriverError::ResponseMismatch("public key").into()),
        }
    }
}

/// Generic struct containing the response fields
/// returned by Exasol in case of an error.
///
/// These errors are directly issued by the Exasol database.
#[derive(Debug, Deserialize, Serialize)]
pub struct ExaError {
    text: String,
    #[serde(rename = "sqlCode")]
    code: String,
}

impl Display for ExaError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", json!(self))
    }
}

impl std::error::Error for ExaError {}

/// Struct used for deserialization of the JSON
/// returned after executing one or more queries
/// Represents the collection of results from all queries.
#[allow(unused)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Results {
    num_results: u8,
    pub results: Vec<QueryResult>,
}

/// Struct used for deserialization of fetched data
/// from getting a result set given a statement handle
#[derive(Debug, Deserialize)]
pub struct FetchedData {
    #[serde(rename = "numRows")]
    pub chunk_rows_num: usize,
    #[serde(skip)]
    pub chunk_rows_pos: usize,
    #[serde(default, deserialize_with = "to_row_major")]
    pub data: Vec<Value>,
}

/// Struct representing a prepared statement
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PreparedStatement {
    statement_handle: u16,
    parameter_data: Option<ParameterData>,
}

impl PreparedStatement {
    /// Returns the prepared statement's database handle
    pub fn handle(&self) -> u16 {
        self.statement_handle
    }

    /// Returns an optional reference to the prepared statement's parameters
    pub fn params(&self) -> Option<&ParameterData> {
        self.parameter_data.as_ref()
    }

    /// Updates the optional parameters' names with the names from the given Vec.
    pub(crate) fn update_param_names(&mut self, names: Vec<String>) {
        if let Some(p) = self.parameter_data.as_mut() {
            p.update_names(names);
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ParameterData {
    num_columns: u8,
    columns: Vec<Column>,
}

impl ParameterData {
    /// Returns the number of parameter columns
    pub fn num_columns(&self) -> u8 {
        self.num_columns
    }

    /// Returns a slice of the parameter columns Vec
    pub fn columns(&self) -> &[Column] {
        self.columns.as_slice()
    }

    /// Updates column names based on the provided Vec.
    pub(crate) fn update_names(&mut self, names: Vec<String>) {
        zip(&mut self.columns, names).for_each(|(c, name)| c.set_name(name));
    }
}

/// Struct representing attributes returned from Exasol.
/// These can either be returned by an explicit `getAttributes` call
/// or as part of any response.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Attributes {
    #[serde(flatten)]
    pub map: HashMap<String, Value>,
}

/// Struct representing database information returned
/// after establishing a connection.
#[allow(unused)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoginInfo {
    protocol_version: ProtocolVersion,
    #[serde(flatten)]
    map: HashMap<String, Value>,
}

/// Struct representing the hosts of the Exasol cluster
#[allow(unused)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Hosts {
    num_nodes: usize,
    nodes: Vec<String>,
}

/// Struct representing public key information
/// returned as part of the login process.
#[allow(unused)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicKey {
    public_key_exponent: String,
    public_key_modulus: String,
    public_key_pem: String,
}

impl From<PublicKey> for String {
    fn from(pub_key: PublicKey) -> Self {
        pub_key.public_key_pem
    }
}

/// Struct used for deserialization of the JSON
/// returned sending queries to the database.
/// Represents the result of one query.
#[allow(non_snake_case)]
#[derive(Debug, Deserialize)]
#[serde(tag = "resultType", rename_all = "camelCase")]
pub enum QueryResultDe {
    #[serde(rename_all = "camelCase")]
    ResultSet { result_set: ResultSet },
    #[serde(rename_all = "camelCase")]
    RowCount { row_count: usize },
}

/// Struct used for deserialization of a ResultSet
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResultSetDe {
    #[serde(rename = "numRows")]
    pub total_rows_num: usize,
    #[serde(rename = "numRowsInMessage")]
    pub chunk_rows_num: usize,
    pub num_columns: usize,
    pub result_set_handle: Option<u16>,
    pub columns: Vec<Column>,
    #[serde(default, deserialize_with = "to_row_major")]
    pub data: Vec<Value>,
}

/// Struct containing the name and datatype (as seen in Exasol) of a given column.
#[allow(unused)]
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Column {
    name: String,
    #[serde(rename = "dataType")]
    datatype: DataType,
}

impl Column {
    /// Sets the column name
    /// Could be used for changing the deserialization name to match
    /// a certain struct without resorting to Serde
    pub fn set_name(&mut self, name: String) {
        self.name = name;
    }

    /// Returns a reference to the column name
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Returns a reference to the column datatype
    pub fn datatype(&self) -> &DataType {
        &self.datatype
    }

    /// Turns the name of this column into lowercase
    pub(crate) fn use_lowercase_name(&mut self) {
        self.set_name(self.name.to_lowercase())
    }
}

impl Display for Column {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.name, self.datatype)
    }
}

/// Struct representing a datatype for a column in a result set.
#[allow(unused)]
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DataType {
    #[serde(rename = "type")]
    type_name: String,
    precision: Option<u8>,
    scale: Option<u8>,
    size: Option<usize>,
    character_set: Option<String>,
    with_local_time_zone: Option<bool>,
    fraction: Option<usize>,
    srid: Option<usize>,
}

impl Display for DataType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.type_name)
    }
}

impl DataType {
    /// Returns a reference ot the datatype name
    pub fn type_name(&self) -> &str {
        self.type_name.as_str()
    }

    /// Returns an optional datatype precision
    pub fn precision(&self) -> Option<u8> {
        self.precision
    }

    /// Returns an optional datatype scale
    pub fn scale(&self) -> Option<u8> {
        self.scale
    }

    /// Returns an optional datatype size
    pub fn size(&self) -> Option<usize> {
        self.size
    }

    /// Returns an optional datatype character set
    pub fn character_set(&self) -> Option<&str> {
        self.character_set.as_deref()
    }

    /// Returns an optional bool for the datatype representing whether the local time zone is used
    pub fn with_local_time_zone(&self) -> Option<bool> {
        self.with_local_time_zone
    }

    /// Returns an optional datatype fraction
    pub fn fraction(&self) -> Option<usize> {
        self.fraction
    }

    /// Returns an optional datatype srid
    pub fn srid(&self) -> Option<usize> {
        self.srid
    }
}

/// Deserialization function used to turn Exasol's
/// column major data into a flattened Vec.
///
/// A row can then be retrieved by getting an item at 0..number_of_columns * row_count.
fn to_row_major<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Vec<Value>, D::Error> {
    struct Column<'a>(&'a mut Vec<Value>);

    impl<'de, 'a> DeserializeSeed<'de> for Column<'a> {
        type Value = ();

        fn deserialize<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
        where
            D: Deserializer<'de>,
        {
            deserializer.deserialize_seq(ColumnVisitor(self.0))
        }
    }

    struct ColumnVisitor<'a>(&'a mut Vec<Value>);

    impl<'de, 'a> Visitor<'de> for ColumnVisitor<'a> {
        type Value = ();

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            write!(formatter, "An array")
        }

        fn visit_seq<A>(self, mut seq: A) -> std::result::Result<(), A::Error>
        where
            A: SeqAccess<'de>,
        {
            while let Some(elem) = seq.next_element()? {
                self.0.push(elem)
            }
            Ok(())
        }
    }

    struct DataVisitor;

    impl<'de> Visitor<'de> for DataVisitor {
        type Value = Vec<serde_json::Value>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            write!(formatter, "An array of arrays")
        }

        fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut transposed = Vec::new();

            while seq.next_element_seed(Column(&mut transposed))?.is_some() {}
            Ok(transposed)
        }
    }

    deserializer.deserialize_seq(DataVisitor)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    #[allow(unused)]
    fn deserialize_error() {
        let result = json!(
            {
                "code": "123",
                "text": "Test"
            }
        );
    }

    #[test]
    #[allow(unused)]
    fn deserialize_results() {
        let result = json!(
            {
               "numResults":1,
               "results":[
                  {
                     "resultSet":{
                        "columns":[
                           {
                              "dataType":{
                                 "precision":1,
                                 "scale":0,
                                 "type":"DECIMAL"
                              },
                              "name":"1"
                           }
                        ],
                        "data":[
                           [
                              1
                           ]
                        ],
                        "numColumns":1,
                        "numRows":1,
                        "numRowsInMessage":1
                     },
                     "resultType":"resultSet"
                  }
               ]
            }
        );
        let de: Results = serde_json::from_value(result).unwrap();
    }

    #[test]
    #[allow(unused)]
    fn deser_fetched_data() {
        let json_data = json!(
            {
                "numRows": 30,
                "data": [[1, 2, 3], [4, 5, 6]]
            }
        );

        let de: FetchedData = serde_json::from_value(json_data).unwrap();
    }

    #[test]
    #[allow(unused)]
    fn deserialize_prepared() {
        let result = json!(
            {
               "statementHandle":1,
               "parameterData":{
                  "numColumns":10,
                  "columns":[
                     {
                        "dataType":{
                           "precision":1,
                           "scale":0,
                           "type":"DECIMAL"
                        },
                        "name":"1"
                     }
                  ]
               }
            }
        );
        let de: PreparedStatement = serde_json::from_value(result).unwrap();
    }

    #[test]
    #[allow(unused)]
    fn deser_param_data() {
        let json_data = json!(
            {
               "numColumns":10,
               "columns":[
                  {
                     "dataType":{
                        "precision":1,
                        "scale":0,
                        "type":"DECIMAL"
                     },
                     "name":"1"
                  }
               ]
            }
        );

        let x: Attributes = serde_json::from_value(json_data).unwrap();
    }

    #[test]
    #[allow(unused)]
    fn deser_attributes() {
        let json_data = json!(
            {
                "key1": "val1",
                "key2": "val2",
                "key3": "val3"
            }
        );

        let x: Attributes = serde_json::from_value(json_data).unwrap();
    }

    #[test]
    #[allow(unused)]
    fn deser_public_key() {
        let json_data = json!(
            {
               "publicKeyExponent":"test1",
               "publicKeyModulus":"test2",
               "publicKeyPem":"test3"
            }
        );

        let x: PublicKey = serde_json::from_value(json_data).unwrap();
    }

    #[test]
    #[allow(unused)]
    fn deser_login_info() {
        let json_data = json!(
            {
                "protocolVersion": 3,
                "key1": "val1",
                "key2": "val2"
            }
        );

        let x: LoginInfo = serde_json::from_value(json_data).unwrap();
    }

    #[test]
    #[allow(unused)]
    fn deser_query_result1() {
        let json_data = json!(
            {
               "resultSet":{
                  "columns":[
                     {
                        "dataType":{
                           "precision":1,
                           "scale":0,
                           "type":"DECIMAL"
                        },
                        "name":"1"
                     }
                  ],
                  "data":[
                     [
                        1
                     ]
                  ],
                  "numColumns":1,
                  "numRows":1,
                  "numRowsInMessage":1
               },
               "resultType":"resultSet"
            }
        );

        let de: QueryResultDe = serde_json::from_value(json_data).unwrap();
    }

    #[test]
    #[allow(unused)]
    fn deser_query_result2() {
        let json_data = json!(
            {
                "resultType": "rowCount",
                "rowCount": 0
            }
        );

        let de: QueryResultDe = serde_json::from_value(json_data).unwrap();
    }

    #[test]
    #[allow(unused)]
    fn deser_result_set() {
        let json_data = json!(
            {
               "columns":[
                  {
                     "dataType":{
                        "precision":1,
                        "scale":0,
                        "type":"DECIMAL"
                     },
                     "name":"1"
                  }
               ],
               "data":[
                  [
                     1
                  ]
               ],
               "numColumns":1,
               "numRows":1,
               "numRowsInMessage":1
            }
        );

        let de: ResultSetDe = serde_json::from_value(json_data).unwrap();
    }

    #[test]
    #[allow(unused)]
    fn deser_column() {
        let json_data = json!(
            {
               "dataType":{
                  "precision":1,
                  "scale":0,
                  "type":"DECIMAL"
               },
               "name":"1"
            }
        );

        let de: Column = serde_json::from_value(json_data).unwrap();
    }

    #[test]
    #[allow(unused)]
    fn deser_datatype() {
        let json_data = json!(
            {
               "precision":1,
               "scale":0,
               "type":"DECIMAL"
            }
        );

        let de: DataType = serde_json::from_value(json_data).unwrap();
    }

    #[test]
    fn deser_to_row_major() {
        let json_data = json!([[1, 2, 3], [1, 2, 3]]);
        let row_major_data = to_row_major(json_data).unwrap();
        assert_eq!(row_major_data, vec![1, 2, 3, 1, 2, 3]);
    }
}
