#![cfg(feature = "migrate")]
#![cfg(feature = "ascii")]

use sqlx_exasol::types::{AsciiStr, AsciiString};

mod macros;

test_type_valid!(varchar_ascii<AsciiString>::"VARCHAR(100) ASCII"::("'first value'" => AsciiStr::from_ascii("first value").unwrap(), "'second value'" => AsciiStr::from_ascii("second value").unwrap()));
test_type_valid!(char_ascii<AsciiString>::"CHAR(10) ASCII"::("'first     '" => AsciiStr::from_ascii("first     ").unwrap(), "'second '" => AsciiStr::from_ascii("second    ").unwrap()));

test_type_valid!(varchar_option<Option<AsciiString>>::"VARCHAR(10) ASCII"::("''" => None::<AsciiString>, "NULL" => None::<AsciiString>, "'value'" => Some(AsciiString::from_ascii("value").unwrap())));
test_type_valid!(char_option<Option<AsciiString>>::"CHAR(10) ASCII"::("''" => None::<AsciiString>, "NULL" => None::<AsciiString>, "'value'" => Some(AsciiString::from_ascii("value     ").unwrap())));

test_type_array!(varchar_array<AsciiString>::"VARCHAR(10) ASCII"::(vec![AsciiString::from_ascii("abc").unwrap(), AsciiString::from_ascii("cde").unwrap()]));
test_type_array!(char_array<AsciiString>::"CHAR(10) ASCII"::(vec![AsciiString::from_ascii("abc").unwrap(), AsciiString::from_ascii("cde").unwrap()]));
