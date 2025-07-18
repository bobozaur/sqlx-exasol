#![cfg(feature = "migrate")]
#![allow(clippy::float_cmp)]

mod macros;

test_type_valid!(f32::"DOUBLE PRECISION"::(f32::MIN, f32::MAX));
test_type_valid!(f64::"DOUBLE PRECISION"::(-3.402_823_466_385_29e38_f64, 3.402_823_466_385_29e38_f64));

test_type_valid!(f32_decimal<f32>::"DECIMAL(36, 16)"::(-1005.0456, 1005.0456, -7462.0, 7462.0));
test_type_valid!(f64_decimal<f64>::"DECIMAL(36, 16)"::(-1_005_213.045_654_3, 1_005_213.045_654_3, -1005.0456, 1005.0456, -7462.0, 7462.0));

test_type_valid!(f64_option<Option<f64>>::"DOUBLE PRECISION"::("NULL" => None::<f64>, -1_005_213.045_654_3 => Some(-1_005_213.045_654_3)));
test_type_valid!(f64_decimal_option<Option<f64>>::"DECIMAL(36, 16)"::("NULL" => None::<f64>, -1_005_213.045_654_3 => Some(-1_005_213.045_654_3)));

test_type_array!(f64_array<f64>::"DOUBLE PRECISION"::(vec![-1_005_213.045_654_3, 1_005_213.045_654_3, -1005.0456, 1005.0456, -7462.0, 7462.0]));
test_type_array!(f64_decimal_array<f64>::"DECIMAL(36, 16)"::(vec![-1_005_213.045_654_3, 1_005_213.045_654_3, -1005.0456, 1005.0456, -7462.0, 7462.0]));
