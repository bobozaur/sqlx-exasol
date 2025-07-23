use sqlx_exasol::types::{ExaIntervalDayToSecond, ExaIntervalYearToMonth};

mod macros;

// test_type_valid!(duration<ExaIntervalDayToSecond>::"INTERVAL DAY TO SECOND"::("'10 20:45:50.123'"
// => ExaIntervalDayToSecond { days: 10, hours: 20, minutes: 45, seconds: 50, milliseconds: 123 },
// "'-10 20:45:50.123'" => ExaIntervalDayToSecond { days: 10, hours: 20, minutes: 45, seconds: 50,
// milliseconds: 123 }); test_type_valid!(duration_str<String>::"INTERVAL DAY TO SECOND"::("'10
// 20:45:50.123'" => "+10 20:45:50.123")); test_type_valid!
// (duration_with_prec<ExaIntervalDayToSecond>::"INTERVAL DAY(4) TO SECOND"::("'10 20:45:50.123'" =>
// ExaIntervalDayToSecond::try_milliseconds(938_750_123).unwrap(), "'-10 20:45:50.123'" =>
// ExaIntervalDayToSecond::try_milliseconds(-938_750_123).unwrap())); test_type_valid!
// (duration_option<Option<ExaIntervalDayToSecond>>::"INTERVAL DAY TO SECOND"::("NULL" =>
// None::<ExaIntervalDayToSecond>, "''" => None::<ExaIntervalDayToSecond>, "'10 20:45:50.123'" =>
// Some(ExaIntervalDayToSecond::try_milliseconds(938_750_123).unwrap()))); test_type_array!
// (duration_array<ExaIntervalDayToSecond>::"INTERVAL DAY TO SECOND"::(vec!["10 20:45:50.123", "10
// 20:45:50.123", "10 20:45:50.123"]));

// test_type_valid!(months<ExaIntervalYearToMonth>::"INTERVAL YEAR TO MONTH"::("'1-5'" =>
// ExaIntervalYearToMonth::new(17), "'-1-5'" => ExaIntervalYearToMonth::new(-17))); test_type_valid!
// (months_str<String>::"INTERVAL YEAR TO MONTH"::("'1-5'" => "+01-05")); test_type_valid!
// (months_with_prec<ExaIntervalYearToMonth>::"INTERVAL YEAR(4) TO MONTH"::("'1000-5'" =>
// ExaIntervalYearToMonth::new(12005), "'-1000-5'" => ExaIntervalYearToMonth::new(-12005)));
// test_type_valid!(months_option<Option<ExaIntervalYearToMonth>>::"INTERVAL YEAR TO MONTH"::("NULL"
// => None::<ExaIntervalYearToMonth>, "''" => None::<ExaIntervalYearToMonth>, "'1-5'" =>
// Some(ExaIntervalYearToMonth::new(17)))); test_type_array!(months_array<ExaIntervalYearToMonth>::"
// INTERVAL YEAR TO MONTH"::(vec!["1-5", "1-5", "1-5"]));
