use std::{borrow::Cow, num::NonZeroUsize, ops::Not};

use serde::{Deserialize, Deserializer, Serialize};

/// Struct representing attributes related to the connection with the Exasol server.
/// These can either be returned by an explicit `getAttributes` call or as part of any response.
///
/// Note that some of these are *read-only*!
/// See the [specification](<https://github.com/exasol/websocket-api/blob/master/docs/WebsocketAPIV1.md#attributes-session-and-database-properties>)
/// for more details.
///
/// Moreover, we store in this other custom connection related attributes, specific to the driver.
#[derive(Clone, Debug)]
pub struct ExaAttributes {
    read_write: ExaRwAttributes<'static>,
    read_only: ExaRoAttributes,
    driver: ExaDriverAttributes,
}

impl ExaAttributes {
    pub(crate) fn new(
        compression_enabled: bool,
        fetch_size: usize,
        encryption_enabled: bool,
        statement_cache_capacity: NonZeroUsize,
    ) -> Self {
        Self {
            read_write: ExaRwAttributes::default(),
            read_only: ExaRoAttributes::new(compression_enabled),
            driver: ExaDriverAttributes::new(
                fetch_size,
                encryption_enabled,
                statement_cache_capacity,
            ),
        }
    }

    #[must_use]
    pub fn autocommit(&self) -> bool {
        self.read_write.autocommit
    }

    #[deprecated = "use Connection::begin() to start a transaction"]
    pub fn set_autocommit(&mut self, autocommit: bool) -> &mut Self {
        self.driver.needs_send = true;
        self.read_write.autocommit = autocommit;
        self.driver.open_transaction = !autocommit;
        self
    }

    #[must_use]
    pub fn current_schema(&self) -> Option<&str> {
        self.read_write.current_schema.as_deref()
    }

    /// Note that setting the open schema to `None` cannot be done
    /// through this attribute. It can only be changed to a different one.
    ///
    /// An explicit `CLOSE SCHEMA;` statement would have to be executed
    /// to accomplish the `no open schema` behavior.
    pub fn set_current_schema(&mut self, schema: String) -> &mut Self {
        self.driver.needs_send = true;
        self.read_write.current_schema = Some(schema.into());
        self
    }

    #[must_use]
    pub fn feedback_interval(&self) -> u64 {
        self.read_write.feedback_interval
    }

    pub fn set_feedback_interval(&mut self, feedback_interval: u64) -> &mut Self {
        self.driver.needs_send = true;
        self.read_write.feedback_interval = feedback_interval;
        self
    }

    #[must_use]
    pub fn numeric_characters(&self) -> &str {
        &self.read_write.numeric_characters
    }

    pub fn set_numeric_characters(&mut self, numeric_characters: String) -> &mut Self {
        self.driver.needs_send = true;
        self.read_write.numeric_characters = numeric_characters.into();
        self
    }

    #[must_use]
    pub fn query_timeout(&self) -> u64 {
        self.read_write.query_timeout
    }

    #[must_use]
    pub fn set_query_timeout(&mut self, query_timeout: u64) -> &mut Self {
        self.driver.needs_send = true;
        self.read_write.query_timeout = query_timeout;
        self
    }

    #[must_use]
    pub fn snapshot_transactions_enabled(&self) -> bool {
        self.read_write.snapshot_transactions_enabled
    }

    pub fn set_snapshot_transactions_enabled(&mut self, enabled: bool) -> &mut Self {
        self.driver.needs_send = true;
        self.read_write.snapshot_transactions_enabled = enabled;
        self
    }

    #[must_use]
    pub fn timestamp_utc_enabled(&self) -> bool {
        self.read_write.timestamp_utc_enabled
    }

    pub fn set_timestamp_utc_enabled(&mut self, enabled: bool) -> &mut Self {
        self.driver.needs_send = true;
        self.read_write.timestamp_utc_enabled = enabled;
        self
    }

    #[must_use]
    pub fn compression_enabled(&self) -> bool {
        self.read_only.compression_enabled
    }

    #[must_use]
    pub fn date_format(&self) -> &str {
        &self.read_only.date_format
    }

    #[must_use]
    pub fn date_language(&self) -> &str {
        &self.read_only.date_language
    }

    #[must_use]
    pub fn datetime_format(&self) -> &str {
        &self.read_only.datetime_format
    }

    #[must_use]
    pub fn default_like_escape_character(&self) -> &str {
        &self.read_only.default_like_escape_character
    }

    #[must_use]
    pub fn timezone(&self) -> &str {
        &self.read_only.timezone
    }

    #[must_use]
    pub fn timezone_behavior(&self) -> &str {
        &self.read_only.timezone_behavior
    }

    #[must_use]
    pub fn open_transaction(&self) -> bool {
        self.driver.open_transaction
    }

    #[must_use]
    pub fn fetch_size(&self) -> usize {
        self.driver.fetch_size
    }

    pub fn set_fetch_size(&mut self, fetch_size: usize) -> &mut Self {
        self.driver.fetch_size = fetch_size;
        self
    }

    #[must_use]
    pub fn encryption_enabled(&self) -> bool {
        self.driver.encryption_enabled
    }

    #[must_use]
    pub fn statement_cache_capacity(&self) -> NonZeroUsize {
        self.driver.statement_cache_capacity
    }

    pub(crate) fn needs_send(&self) -> bool {
        self.driver.needs_send
    }

    pub(crate) fn set_needs_send(&mut self, flag: bool) -> &mut Self {
        self.driver.needs_send = flag;
        self
    }

    pub(crate) fn read_write(&self) -> &ExaRwAttributes<'static> {
        &self.read_write
    }

    pub(crate) fn update(&mut self, other: ExaAttributesOpt) {
        macro_rules! other_or_prev {
            ($kind:tt, $field:tt) => {
                if let Some(new) = other.$field {
                    self.$kind.$field = new.into();
                }
            };
        }

        self.read_write.current_schema = other.current_schema.map(From::from);

        other_or_prev!(read_write, autocommit);
        other_or_prev!(read_write, feedback_interval);
        other_or_prev!(read_write, numeric_characters);
        other_or_prev!(read_write, query_timeout);
        other_or_prev!(read_write, snapshot_transactions_enabled);
        other_or_prev!(read_write, timestamp_utc_enabled);
        other_or_prev!(read_only, compression_enabled);
        other_or_prev!(read_only, date_format);
        other_or_prev!(read_only, date_language);
        other_or_prev!(read_only, datetime_format);
        other_or_prev!(read_only, default_like_escape_character);
        other_or_prev!(read_only, timezone);
        other_or_prev!(read_only, timezone_behavior);
    }
}

/// Database read-write attributes
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExaRwAttributes<'a> {
    // The attribute can only change the open schema, it cannot close it, hence the skip. However,
    // if no schema is currently open Exasol returns `null`.
    #[serde(skip_serializing_if = "Option::is_none")]
    current_schema: Option<Cow<'a, str>>,
    autocommit: bool,
    feedback_interval: u64,
    numeric_characters: Cow<'a, str>,
    query_timeout: u64,
    snapshot_transactions_enabled: bool,
    timestamp_utc_enabled: bool,
}

impl<'a> ExaRwAttributes<'a> {
    pub(crate) fn new(
        current_schema: Option<Cow<'a, str>>,
        feedback_interval: u64,
        query_timeout: u64,
    ) -> Self {
        Self {
            current_schema,
            feedback_interval,
            query_timeout,
            ..Default::default()
        }
    }
}

impl<'a> Default for ExaRwAttributes<'a> {
    fn default() -> Self {
        Self {
            autocommit: true,
            current_schema: None,
            feedback_interval: 1,
            numeric_characters: Cow::Owned(".,".into()),
            query_timeout: 0,
            snapshot_transactions_enabled: false,
            timestamp_utc_enabled: false,
        }
    }
}

#[derive(Clone, Debug)]
struct ExaRoAttributes {
    compression_enabled: bool,
    date_format: String,
    date_language: String,
    datetime_format: String,
    default_like_escape_character: String,
    timezone: String,
    timezone_behavior: String,
}

impl ExaRoAttributes {
    fn new(compression_enabled: bool) -> Self {
        Self {
            compression_enabled,
            date_format: "YYYY-MM-DD".to_owned(),
            date_language: "ENG".to_owned(),
            datetime_format: "YYYY-MM-DD HH24:MI:SS.FF6".to_owned(),
            default_like_escape_character: "\\".to_owned(),
            timezone: "UNIVERSAL".to_owned(),
            timezone_behavior: "INVALID SHIFT AMBIGUOUS ST".to_owned(),
        }
    }
}

#[derive(Clone, Debug)]
struct ExaDriverAttributes {
    // This is technically a read-only attribute, but Exasol doesn't seem to correctly set or even
    // return it. We therefore control it manually.
    open_transaction: bool,
    needs_send: bool,
    fetch_size: usize,
    encryption_enabled: bool,
    statement_cache_capacity: NonZeroUsize,
}

impl ExaDriverAttributes {
    fn new(
        fetch_size: usize,
        encryption_enabled: bool,
        statement_cache_capacity: NonZeroUsize,
    ) -> Self {
        Self {
            open_transaction: false,
            needs_send: false,
            fetch_size,
            encryption_enabled,
            statement_cache_capacity,
        }
    }
}

/// Helper type representing attributes returned by Exasol.
///
/// While [`ExaAttributes`] are stored by connections and sent to the database, this type is used
/// for deserialization of the attributes that Exasol sends to us.
///
/// The returned attributes are then used to update a connection's [`ExaAttributes`].
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExaAttributesOpt {
    // ##########################################################
    // ############# Database read-write attributes #############
    // ##########################################################
    autocommit: Option<bool>,
    #[serde(default)]
    #[serde(deserialize_with = "ExaAttributesOpt::deserialize_current_schema")]
    current_schema: Option<String>,
    feedback_interval: Option<u64>,
    numeric_characters: Option<String>,
    query_timeout: Option<u64>,
    snapshot_transactions_enabled: Option<bool>,
    timestamp_utc_enabled: Option<bool>,
    // ##########################################################
    // ############# Database read-only attributes ##############
    // ##########################################################
    compression_enabled: Option<bool>,
    date_format: Option<String>,
    date_language: Option<String>,
    datetime_format: Option<String>,
    default_like_escape_character: Option<String>,
    timezone: Option<String>,
    timezone_behavior: Option<String>,
}

impl ExaAttributesOpt {
    /// Helper function because Exasol returns an empty string if no schema was selected.
    fn deserialize_current_schema<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let Some(value) = Option::deserialize(deserializer)? else {
            return Ok(None);
        };

        Ok(String::is_empty(&value).not().then_some(value))
    }
}
