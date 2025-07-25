#![cfg(feature = "migrate")]

mod macros;

test_type_valid!(varchar_utf8<String>::"VARCHAR(100) UTF8"::("'first value 🦀'" => "first value 🦀", "'second value 🦀'" => "second value 🦀"));
test_type_valid!(char_utf8<String>::"CHAR(10) UTF8"::("'first 🦀   '" => "first 🦀   ", "'second 🦀'" => "second 🦀  "));

test_type_valid!(varchar_option<Option<String>>::"VARCHAR(10) UTF8"::("''" => None::<String>, "NULL" => None::<String>, "'value'" => Some("value".to_owned())));
test_type_valid!(char_option<Option<String>>::"CHAR(10) UTF8"::("''" => None::<String>, "NULL" => None::<String>, "'value'" => Some("value     ".to_owned())));

test_type_array!(varchar_array<String>::"VARCHAR(10) UTF8"::(vec!["abc".to_string(), "cde".to_string()]));
test_type_array!(char_array<String>::"CHAR(10) UTF8"::(vec!["abc".to_string(), "cde".to_string()]));
