use serde::{Deserialize, Serialize};
use sqlx_exasol::types::Json;

use crate::{test_type_array, test_type_valid};

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
struct Test {
    field: String,
}

test_type_valid!(json_utf8<Json<Test>>::"VARCHAR(100) UTF8"::(r#"'{"field": "🦀"}'"# => Json(Test{field: String::from("🦀")})));
test_type_valid!(json_ascii<Json<Test>>::"VARCHAR(100) ASCII"::(r#"'{"field": "stuff"}'"# => Json(Test{field: String::from("stuff")})));
test_type_array!(json_utf8_array<Json<Test>>::"VARCHAR(100) UTF8"::(vec![Json(Test{field: String::from("🦀")}), Json(Test{field: String::from("🦀")})]));
