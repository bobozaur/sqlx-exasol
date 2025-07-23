#![cfg(feature = "migrate")]
#![cfg(feature = "chrono")]

mod macros;

use sqlx_exasol::types::chrono::{DateTime, Local, NaiveDate, NaiveDateTime, Utc};

const TIMESTAMP_FMT: &str = "%Y-%m-%d %H:%M:%S%.9f";
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
