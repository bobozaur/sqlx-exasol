#![cfg(feature = "migrate")]

mod macros;

use sqlx_exasol::types::ExaIntervalYearToMonth;

test_type_valid!(months<ExaIntervalYearToMonth>::"INTERVAL YEAR TO MONTH"::("'1-5'" => ExaIntervalYearToMonth(17), "'-1-5'" => ExaIntervalYearToMonth(-17)));
test_type_valid!(months_with_prec<ExaIntervalYearToMonth>::"INTERVAL YEAR(4) TO MONTH"::("'1000-5'" => ExaIntervalYearToMonth(12005), "'-1000-5'" => ExaIntervalYearToMonth(-12005)));
test_type_valid!(months_option<Option<ExaIntervalYearToMonth>>::"INTERVAL YEAR TO MONTH"::("NULL" => None::<ExaIntervalYearToMonth>, "''" => None::<ExaIntervalYearToMonth>, "'1-5'" => Some(ExaIntervalYearToMonth(17))));
test_type_array!(months_array<ExaIntervalYearToMonth>::"INTERVAL YEAR TO MONTH"::(vec![ExaIntervalYearToMonth (17), ExaIntervalYearToMonth (17), ExaIntervalYearToMonth (17)]));
