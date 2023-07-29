use std::fmt::Display;

use serde::{Deserialize, Serialize};
use sqlx_core::type_info::TypeInfo;

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "UPPERCASE")]
#[serde(tag = "type")]
pub enum ExaTypeInfo {
    Boolean,
    Char(StringLike),
    Date,
    Decimal(Decimal),
    Double,
    Geometry(Geometry),
    #[serde(rename = "INTERVAL DAY TO SECOND")]
    IntervalDayToSecond(IntervalDayToSecond),
    #[serde(rename = "INTERVAL YEAR TO MONTH")]
    IntervalYearToMonth(IntervalYearToMonth),
    Timestamp,
    #[serde(rename = "TIMESTAMP WITH LOCAL TIME ZONE")]
    TimestampWithLocalTimeZone,
    Varchar(StringLike),
    Hashtype(Hashtype),
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct StringLike {
    character_set: String,
}

impl Default for StringLike {
    fn default() -> Self {
        Self {
            character_set: Self::DEFAULT_CHARSET.to_owned(),
        }
    }
}

impl StringLike {
    const DEFAULT_CHARSET: &str = "UTF8";

    pub fn new(character_set: String) -> Self {
        Self { character_set }
    }

    pub fn character_set(&self) -> &str {
        &self.character_set
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Decimal {
    precision: u8,
    scale: u8,
}

impl Decimal {
    pub fn new(precision: u8, scale: u8) -> Self {
        Self { precision, scale }
    }

    pub fn precision(&self) -> u8 {
        self.precision
    }

    pub fn scale(&self) -> u8 {
        self.scale
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Geometry {
    srid: u16,
}

impl Geometry {
    pub fn new(srid: u16) -> Self {
        Self { srid }
    }

    pub fn srid(&self) -> u16 {
        self.srid
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct IntervalDayToSecond {
    precision: u8,
    fraction: u8,
}

impl IntervalDayToSecond {
    pub fn new(precision: u8, fraction: u8) -> Self {
        Self {
            precision,
            fraction,
        }
    }

    pub fn precision(&self) -> u8 {
        self.precision
    }

    pub fn fraction(&self) -> u8 {
        self.fraction
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct IntervalYearToMonth {
    precision: u8,
}

impl IntervalYearToMonth {
    pub fn new(precision: u8) -> Self {
        Self { precision }
    }

    pub fn precision(&self) -> u8 {
        self.precision
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Hashtype {
    size: u16,
}

impl Hashtype {
    pub fn new(size: u16) -> Self {
        Self { size }
    }

    pub fn size(&self) -> u16 {
        self.size
    }
}

impl AsRef<str> for ExaTypeInfo {
    fn as_ref(&self) -> &str {
        match self {
            ExaTypeInfo::Boolean => "BOOLEAN",
            ExaTypeInfo::Char(_) => "CHAR",
            ExaTypeInfo::Date => "DATE",
            ExaTypeInfo::Decimal(_) => "DECIMAL",
            ExaTypeInfo::Double => "DOUBLE PRECISION",
            ExaTypeInfo::Geometry(_) => "GEOMETRY",
            ExaTypeInfo::IntervalDayToSecond(_) => "INTERVAL DAY TO SECOND",
            ExaTypeInfo::IntervalYearToMonth(_) => "INTERVAL YEAR TO MONTH",
            ExaTypeInfo::Timestamp => "TIMESTAMP",
            ExaTypeInfo::TimestampWithLocalTimeZone => "TIMESTAMP WITH LOCAL TIME ZONE",
            ExaTypeInfo::Varchar(_) => "VARCHAR",
            ExaTypeInfo::Hashtype(_) => "HASHTYPE",
        }
    }
}

impl TypeInfo for ExaTypeInfo {
    fn is_null(&self) -> bool {
        false
    }

    fn name(&self) -> &str {
        self.as_ref()
    }
}

impl Display for ExaTypeInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

impl Eq for ExaTypeInfo {}

#[cfg(test)]
#[test]
fn test() {
    let input = r#"{
        "status": "ok",
        "responseData": {
          "results": [
            {
              "resultType": "resultSet",
              "resultSet": {
                "numColumns": 12,
                "numRows": 0,
                "numRowsInMessage": 0,
                "columns": [
                  {
                    "name": "BOOLEAN_COLUMN",
                    "dataType": {
                      "type": "BOOLEAN"
                    }
                  },
                  {
                    "name": "CHAR_COLUMN",
                    "dataType": {
                      "type": "CHAR",
                      "size": 100,
                      "characterSet": "UTF8"
                    }
                  },
                  {
                    "name": "DATE_COLUMN",
                    "dataType": {
                      "type": "DATE",
                      "size": 4
                    }
                  },
                  {
                    "name": "DECIMAL_COLUMN",
                    "dataType": {
                      "type": "DECIMAL",
                      "precision": 10,
                      "scale": 5
                    }
                  },
                  {
                    "name": "FLOAT_COLUMN",
                    "dataType": {
                      "type": "DOUBLE"
                    }
                  },
                  {
                    "name": "GEOMETRY_COLUMN",
                    "dataType": {
                      "type": "GEOMETRY",
                      "size": 2000000,
                      "srid": 0
                    }
                  },
                  {
                    "name": "INTERVAL_DAY_COLUMN",
                    "dataType": {
                      "type": "INTERVAL DAY TO SECOND",
                      "size": 16,
                      "precision": 2,
                      "fraction": 3
                    }
                  },
                  {
                    "name": "INTERVAL_YEAR_COLUMN",
                    "dataType": {
                      "type": "INTERVAL YEAR TO MONTH",
                      "size": 6,
                      "precision": 2
                    }
                  },
                  {
                    "name": "TIMESTAMP_COLUMN",
                    "dataType": {
                      "type": "TIMESTAMP",
                      "withLocalTimeZone": false,
                      "size": 8
                    }
                  },
                  {
                    "name": "TIMESTAMP_WITH_TZ_COLUMN",
                    "dataType": {
                      "type": "TIMESTAMP WITH LOCAL TIME ZONE",
                      "withLocalTimeZone": true,
                      "size": 8
                    }
                  },
                  {
                    "name": "VARCHAR_COLUMN",
                    "dataType": {
                      "type": "VARCHAR",
                      "size": 200,
                      "characterSet": "UTF8"
                    }
                  },
                  {
                    "name": "HASHTYPE_COLUMN",
                    "dataType": {
                      "type": "HASHTYPE",
                      "size": 32
                    }
                  }
                ]
              }
            }
          ],
          "numResults": 1
        }
      }"#;
}
