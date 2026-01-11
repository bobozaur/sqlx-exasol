#![cfg(all(feature = "migrate", feature = "chrono"))]

mod macros;

use sqlx_exasol::types::chrono::{DateTime, Local, NaiveDate, NaiveDateTime, TimeDelta, Utc};

const TIMESTAMP_FMT: &str = "%Y-%m-%d %H:%M:%S%.f";
const DATE_FMT: &str = "%Y-%m-%d";

test_type_valid!(naive_datetime<NaiveDateTime>::"TIMESTAMP"::("'2023-08-12 19:22:36.591000'" => NaiveDateTime::parse_from_str("2023-08-12 19:22:36.591000", TIMESTAMP_FMT).unwrap()));
test_type_valid!(naive_datetime_optional<Option<NaiveDateTime>>::"TIMESTAMP"::("NULL" => None::<NaiveDateTime>, "''" => None::<NaiveDateTime>, "'2023-08-12 19:22:36.591000'" => Some(NaiveDateTime::parse_from_str("2023-08-12 19:22:36.591000", TIMESTAMP_FMT).unwrap())));
test_type_array!(naive_datetime_array<NaiveDateTime>::"TIMESTAMP"::(vec![NaiveDateTime::parse_from_str("2023-08-12 19:22:36.591000", TIMESTAMP_FMT).unwrap(), NaiveDateTime::parse_from_str("2023-08-12 19:22:36.591000", TIMESTAMP_FMT).unwrap(), NaiveDateTime::parse_from_str("2023-08-12 19:22:36.591000", TIMESTAMP_FMT).unwrap()]));

test_type_valid!(naive_date<NaiveDate>::"DATE"::("'2023-08-12'" => NaiveDate::parse_from_str("2023-08-12", DATE_FMT).unwrap()));
test_type_valid!(naive_date_option<Option<NaiveDate>>::"DATE"::("NULL" => None::<NaiveDate>, "''" => None::<NaiveDate>, "'2023-08-12'" => Some(NaiveDate::parse_from_str("2023-08-12", DATE_FMT).unwrap())));
test_type_array!(naive_date_array<NaiveDate>::"DATE"::(vec![NaiveDate::parse_from_str("2023-08-12", DATE_FMT).unwrap(), NaiveDate::parse_from_str("2023-08-12", DATE_FMT).unwrap(), NaiveDate::parse_from_str("2023-08-12", DATE_FMT).unwrap()]));

test_type_valid!(datetime_utc<DateTime<Utc>>::"TIMESTAMP"::("'2023-08-12 19:22:36.591000'" => NaiveDateTime::parse_from_str("2023-08-12 19:22:36.591000", TIMESTAMP_FMT).unwrap().and_utc()));
test_type_valid!(datetime_utc_option<Option<DateTime<Utc>>>::"TIMESTAMP"::("NULL" => None::<DateTime<Utc>>, "''" => None::<DateTime<Utc>>, "'2023-08-12 19:22:36.591000'" => Some(NaiveDateTime::parse_from_str("2023-08-12 19:22:36.591000", TIMESTAMP_FMT).unwrap().and_utc())));
test_type_array!(datetime_utc_array<DateTime<Utc>>::"TIMESTAMP"::(vec![NaiveDateTime::parse_from_str("2023-08-12 19:22:36.591000", TIMESTAMP_FMT).unwrap().and_utc(), NaiveDateTime::parse_from_str("2023-08-12 19:22:36.591000", TIMESTAMP_FMT).unwrap().and_utc(), NaiveDateTime::parse_from_str("2023-08-12 19:22:36.591000", TIMESTAMP_FMT).unwrap().and_utc()]));

test_type_valid!(datetime_local<DateTime<Local>>::"TIMESTAMP WITH LOCAL TIME ZONE"::("'2023-08-12 19:22:36.591000'" => NaiveDateTime::parse_from_str("2023-08-12 19:22:36.591000", TIMESTAMP_FMT).unwrap().and_local_timezone(Local).unwrap()));
test_type_valid!(datetime_local_option<Option<DateTime<Local>>>::"TIMESTAMP WITH LOCAL TIME ZONE"::("NULL" => None::<DateTime<Local>>, "''" => None::<DateTime<Local>>, "'2023-08-12 19:22:36.591000'" => Some(NaiveDateTime::parse_from_str("2023-08-12 19:22:36.591000", TIMESTAMP_FMT).unwrap().and_local_timezone(Local).unwrap())));
test_type_array!(datetime_local_array<DateTime<Local>>::"TIMESTAMP WITH LOCAL TIME ZONE"::(vec![NaiveDateTime::parse_from_str("2023-08-12 19:22:36.591000", TIMESTAMP_FMT).unwrap().and_local_timezone(Local).unwrap(), NaiveDateTime::parse_from_str("2023-08-12 19:22:36.591000", TIMESTAMP_FMT).unwrap().and_local_timezone(Local).unwrap(), NaiveDateTime::parse_from_str("2023-08-12 19:22:36.591000", TIMESTAMP_FMT).unwrap().and_local_timezone(Local).unwrap()]));

test_type_valid!(duration<TimeDelta>::"INTERVAL DAY TO SECOND"::("'10 20:45:50.123'" => TimeDelta::try_milliseconds(938_750_123).unwrap(), "'-10 20:45:50.123'" => TimeDelta::try_milliseconds(-938_750_123).unwrap()));
test_type_valid!(duration_with_prec<TimeDelta>::"INTERVAL DAY(4) TO SECOND"::("'10 20:45:50.123'" => TimeDelta::try_milliseconds(938_750_123).unwrap(), "'-10 20:45:50.123'" => TimeDelta::try_milliseconds(-938_750_123).unwrap()));
test_type_valid!(duration_option<Option<TimeDelta>>::"INTERVAL DAY TO SECOND"::("NULL" => None::<TimeDelta>, "''" => None::<TimeDelta>, "'10 20:45:50.123'" => Some(TimeDelta::try_milliseconds(938_750_123).unwrap())));
test_type_array!(duration_array<TimeDelta>::"INTERVAL DAY TO SECOND"::(vec!["10 20:45:50.123", "10 20:45:50.123", "10 20:45:50.123"]));
