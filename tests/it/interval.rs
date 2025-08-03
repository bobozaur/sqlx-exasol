use sqlx_exasol::types::{ExaIntervalDayToSecond, ExaIntervalYearToMonth};

use crate::{test_type_array, test_type_valid};

test_type_valid!(duration<ExaIntervalDayToSecond>::"INTERVAL DAY TO SECOND"::("'10 20:45:50.123'" => ExaIntervalDayToSecond { days: 10, hours: 20, minutes: 45, seconds: 50, milliseconds: 123 }, "'-10 20:45:50.123'" => ExaIntervalDayToSecond { days: -10, hours: 20, minutes: 45, seconds: 50, milliseconds: 123 }));
test_type_valid!(duration_with_prec<ExaIntervalDayToSecond>::"INTERVAL DAY(4) TO SECOND"::("'10 20:45:50.123'" => ExaIntervalDayToSecond{ days: 10, hours: 20, minutes: 45, seconds: 50, milliseconds: 123 }, "'-10 20:45:50.123'" => ExaIntervalDayToSecond{ days: -10, hours: 20, minutes: 45, seconds: 50, milliseconds: 123 }));
test_type_valid!(duration_option<Option<ExaIntervalDayToSecond>>::"INTERVAL DAY TO SECOND"::("NULL" => None::<ExaIntervalDayToSecond>, "''" => None::<ExaIntervalDayToSecond>, "'10 20:45:50.123'" => Some(ExaIntervalDayToSecond{ days: 10, hours: 20, minutes: 45, seconds: 50, milliseconds: 123 })));
test_type_array!(duration_array<ExaIntervalDayToSecond>::"INTERVAL DAY TO SECOND"::(vec![ExaIntervalDayToSecond { days: 10, hours: 20, minutes: 45, seconds: 50, milliseconds: 123 }, ExaIntervalDayToSecond { days: 10, hours: 20, minutes: 45, seconds: 50, milliseconds: 123 }, ExaIntervalDayToSecond { days: 10, hours: 20, minutes: 45, seconds: 50, milliseconds: 123 }]));

test_type_valid!(months<ExaIntervalYearToMonth>::"INTERVAL YEAR TO MONTH"::("'1-5'" => ExaIntervalYearToMonth { years: 1, months: 5 }, "'-1-5'" => ExaIntervalYearToMonth { years: -1, months: 5 }));
test_type_valid!(months_with_prec<ExaIntervalYearToMonth>::"INTERVAL YEAR(4) TO MONTH"::("'1000-5'" => ExaIntervalYearToMonth { years: 1000, months: 5 }, "'-1000-5'" => ExaIntervalYearToMonth { years: -1000, months: 5 }));
test_type_valid!(months_option<Option<ExaIntervalYearToMonth>>::"INTERVAL YEAR TO MONTH"::("NULL" => None::<ExaIntervalYearToMonth>, "''" => None::<ExaIntervalYearToMonth>, "'1-5'" => Some(ExaIntervalYearToMonth{ years: 1, months: 5 })));
test_type_array!(months_array<ExaIntervalYearToMonth>::"INTERVAL YEAR TO MONTH"::(vec![ExaIntervalYearToMonth { years: 1, months: 5 }, ExaIntervalYearToMonth { years: 1, months: 5 }, ExaIntervalYearToMonth { years: 1, months: 5 }]));
