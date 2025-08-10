use sqlx_exasol::types::time::{Date, Duration, OffsetDateTime, PrimitiveDateTime};
use time::{format_description::BorrowedFormatItem, macros::format_description};

use crate::{test_type_array, test_type_valid};

const TIMESTAMP_FMT: &[BorrowedFormatItem<'static>] =
    format_description!("[year]-[month]-[day] [hour]:[minute]:[second].[subsecond]");
const DATE_FMT: &[BorrowedFormatItem<'static>] = format_description!("[year]-[month]-[day]");

test_type_valid!(date<Date>::"DATE"::("'2023-08-12'" => Date::parse("2023-08-12", DATE_FMT).unwrap()));
test_type_valid!(date_option<Option<Date>>::"DATE"::("NULL" => None::<Date>, "''" => None::<Date>, "'2023-08-12'" => Some(Date::parse("2023-08-12", DATE_FMT).unwrap())));
test_type_array!(date_array<Date>::"DATE"::(vec![Date::parse("2023-08-12", DATE_FMT).unwrap(), Date::parse("2023-08-12", DATE_FMT).unwrap(), Date::parse("2023-08-12", DATE_FMT).unwrap()]));

test_type_valid!(offset_datetime<OffsetDateTime>::"TIMESTAMP"::("'2023-08-12 19:22:36.591000'" => PrimitiveDateTime::parse("2023-08-12 19:22:36.591000", TIMESTAMP_FMT).unwrap().assume_utc()));
test_type_valid!(offset_datetime_option<Option<OffsetDateTime>>::"TIMESTAMP"::("NULL" => None::<OffsetDateTime>, "''" => None::<OffsetDateTime>, "'2023-08-12 19:22:36.591000'" => Some(PrimitiveDateTime::parse("2023-08-12 19:22:36.591000", TIMESTAMP_FMT).unwrap().assume_utc())));
test_type_array!(offset_datetime_array<OffsetDateTime>::"TIMESTAMP"::(vec![PrimitiveDateTime::parse("2023-08-12 19:22:36.591000", TIMESTAMP_FMT).unwrap().assume_utc(), PrimitiveDateTime::parse("2023-08-12 19:22:36.591000", TIMESTAMP_FMT).unwrap().assume_utc(), PrimitiveDateTime::parse("2023-08-12 19:22:36.591000", TIMESTAMP_FMT).unwrap().assume_utc()]));

test_type_valid!(primitive_datetime<PrimitiveDateTime>::"TIMESTAMP WITH LOCAL TIME ZONE"::("'2023-08-12 19:22:36.591000'" => PrimitiveDateTime::parse("2023-08-12 19:22:36.591000", TIMESTAMP_FMT).unwrap()));
test_type_valid!(primitive_datetime_optional<Option<PrimitiveDateTime>>::"TIMESTAMP WITH LOCAL TIME ZONE"::("NULL" => None::<PrimitiveDateTime>, "''" => None::<PrimitiveDateTime>, "'2023-08-12 19:22:36.591000'" => Some(PrimitiveDateTime::parse("2023-08-12 19:22:36.591000", TIMESTAMP_FMT).unwrap())));
test_type_array!(primitive_datetime_array<PrimitiveDateTime>::"TIMESTAMP WITH LOCAL TIME ZONE"::(vec![PrimitiveDateTime::parse("2023-08-12 19:22:36.591000", TIMESTAMP_FMT).unwrap(), PrimitiveDateTime::parse("2023-08-12 19:22:36.591000", TIMESTAMP_FMT).unwrap(), PrimitiveDateTime::parse("2023-08-12 19:22:36.591000", TIMESTAMP_FMT).unwrap()]));

test_type_valid!(duration<Duration>::"INTERVAL DAY TO SECOND"::("'10 20:45:50.123'" => Duration::milliseconds(938_750_123), "'-10 20:45:50.123'" => Duration::milliseconds(-938_750_123)));
test_type_valid!(duration_with_prec<Duration>::"INTERVAL DAY(4) TO SECOND"::("'10 20:45:50.123'" => Duration::milliseconds(938_750_123), "'-10 20:45:50.123'" => Duration::milliseconds(-938_750_123)));
test_type_valid!(duration_option<Option<Duration>>::"INTERVAL DAY TO SECOND"::("NULL" => None::<Duration>, "''" => None::<Duration>, "'10 20:45:50.123'" => Some(Duration::milliseconds(938_750_123))));
test_type_array!(duration_array<Duration>::"INTERVAL DAY TO SECOND"::(vec!["10 20:45:50.123", "10 20:45:50.123", "10 20:45:50.123"]));
