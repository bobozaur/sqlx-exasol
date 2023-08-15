use super::*;

test_type_valid!(geometry<String>::"GEOMETRY"::("'POINT (1 2)'" => "POINT (1 2)", "'POINT (3 4)'" => "POINT (3 4)"));
test_type_valid!(geometry_option<Option<String>>::"GEOMETRY"::("''" => None::<String>, "NULL" => None::<String>, "'POINT (3 4)'" => Some("POINT (3 4)".to_owned())));
test_type_array!(geometry_array<String>::"GEOMETRY"::(vec!["POINT (1 2)".to_owned(), "POINT (3 4)".to_owned()]));
