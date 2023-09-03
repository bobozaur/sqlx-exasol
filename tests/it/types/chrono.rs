use ::chrono::{DateTime, Duration, Local, NaiveDate, NaiveDateTime, Utc};
use sqlx_exasol::Months;

use super::*;

const TIMESTAMP_FMT: &str = "%Y-%m-%d %H:%M:%S%.6f";
const DATE_FMT: &str = "%Y-%m-%d";

test_type_valid!(naive_datetime<NaiveDateTime>::"TIMESTAMP"::("'2023-08-12 19:22:36.591000'" => NaiveDateTime::parse_from_str("2023-08-12 19:22:36.591000", TIMESTAMP_FMT).unwrap()));
test_type_valid!(naive_datetime_str<String>::"TIMESTAMP"::("'2023-08-12 19:22:36.591000'" => "2023-08-12 19:22:36.591000"));
test_type_valid!(naive_datetime_optional<Option<NaiveDateTime>>::"TIMESTAMP"::("NULL" => None::<NaiveDateTime>, "''" => None::<NaiveDateTime>, "'2023-08-12 19:22:36.591000'" => Some(NaiveDateTime::parse_from_str("2023-08-12 19:22:36.591000", TIMESTAMP_FMT).unwrap())));
test_type_array!(naive_datetime_array<NaiveDateTime>::"TIMESTAMP"::(vec!["2023-08-12 19:22:36.591000", "2023-08-12 19:22:36.591000", "2023-08-12 19:22:36.591000"]));

test_type_valid!(naive_date<NaiveDate>::"DATE"::("'2023-08-12'" => NaiveDate::parse_from_str("2023-08-12", DATE_FMT).unwrap()));
test_type_valid!(naive_date_str<String>::"DATE"::("'2023-08-12'" => "2023-08-12"));
test_type_valid!(naive_date_option<Option<NaiveDate>>::"DATE"::("NULL" => None::<NaiveDate>, "''" => None::<NaiveDate>, "'2023-08-12'" => Some(NaiveDate::parse_from_str("2023-08-12", DATE_FMT).unwrap())));
test_type_array!(naive_date_array<NaiveDate>::"DATE"::(vec!["2023-08-12", "2023-08-12", "2023-08-12"]));

test_type_valid!(datetime_utc<DateTime<Utc>>::"TIMESTAMP"::("'2023-08-12 19:22:36.591000'" => NaiveDateTime::parse_from_str("2023-08-12 19:22:36.591000", TIMESTAMP_FMT).unwrap().and_utc()));
test_type_valid!(datetime_utc_str<String>::"TIMESTAMP"::("'2023-08-12 19:22:36.591000'" => "2023-08-12 19:22:36.591000"));
test_type_valid!(datetime_utc_option<Option<DateTime<Utc>>>::"TIMESTAMP"::("NULL" => None::<DateTime<Utc>>, "''" => None::<DateTime<Utc>>, "'2023-08-12 19:22:36.591000'" => Some(NaiveDateTime::parse_from_str("2023-08-12 19:22:36.591000", TIMESTAMP_FMT).unwrap().and_utc())));
test_type_array!(datetime_utc_array<DateTime<Utc>>::"TIMESTAMP"::(vec!["2023-08-12 19:22:36.591000", "2023-08-12 19:22:36.591000", "2023-08-12 19:22:36.591000"]));

test_type_valid!(datetime_local<DateTime<Local>>::"TIMESTAMP WITH LOCAL TIME ZONE"::("'2023-08-12 19:22:36.591000'" => NaiveDateTime::parse_from_str("2023-08-12 19:22:36.591000", TIMESTAMP_FMT).unwrap().and_local_timezone(Local).unwrap()));
test_type_valid!(datetime_local_str<String>::"TIMESTAMP WITH LOCAL TIME ZONE"::("'2023-08-12 19:22:36.591000'" => "2023-08-12 19:22:36.591000"));
test_type_valid!(datetime_local_option<Option<DateTime<Local>>>::"TIMESTAMP WITH LOCAL TIME ZONE"::("NULL" => None::<DateTime<Local>>, "''" => None::<DateTime<Local>>, "'2023-08-12 19:22:36.591000'" => Some(NaiveDateTime::parse_from_str("2023-08-12 19:22:36.591000", TIMESTAMP_FMT).unwrap().and_local_timezone(Local).unwrap())));
test_type_array!(datetime_local_array<DateTime<Local>>::"TIMESTAMP WITH LOCAL TIME ZONE"::(vec!["2023-08-12 19:22:36.591000", "2023-08-12 19:22:36.591000", "2023-08-12 19:22:36.591000"]));

test_type_valid!(duration<Duration>::"INTERVAL DAY TO SECOND"::("'10 20:45:50.123'" => Duration::milliseconds(938750123), "'-10 20:45:50.123'" => Duration::milliseconds(-938750123)));
test_type_valid!(duration_str<String>::"INTERVAL DAY TO SECOND"::("'10 20:45:50.123'" => "+10 20:45:50.123"));
test_type_valid!(duration_with_prec<Duration>::"INTERVAL DAY(4) TO SECOND"::("'10 20:45:50.123'" => Duration::milliseconds(938750123), "'-10 20:45:50.123'" => Duration::milliseconds(-938750123)));
test_type_valid!(duration_option<Option<Duration>>::"INTERVAL DAY TO SECOND"::("NULL" => None::<Duration>, "''" => None::<Duration>, "'10 20:45:50.123'" => Some(Duration::milliseconds(938750123))));
test_type_array!(duration_array<Duration>::"INTERVAL DAY TO SECOND"::(vec!["10 20:45:50.123", "10 20:45:50.123", "10 20:45:50.123"]));

test_type_valid!(months<Months>::"INTERVAL YEAR TO MONTH"::("'1-5'" => Months::new(17), "'-1-5'" => Months::new(-17)));
test_type_valid!(months_str<String>::"INTERVAL YEAR TO MONTH"::("'1-5'" => "+01-05"));
test_type_valid!(months_with_prec<Months>::"INTERVAL YEAR(4) TO MONTH"::("'1000-5'" => Months::new(12005), "'-1000-5'" => Months::new(-12005)));
test_type_valid!(months_option<Option<Months>>::"INTERVAL YEAR TO MONTH"::("NULL" => None::<Months>, "''" => None::<Months>, "'1-5'" => Some(Months::new(17))));
test_type_array!(months_array<Months>::"INTERVAL YEAR TO MONTH"::(vec!["1-5", "1-5", "1-5"]));
