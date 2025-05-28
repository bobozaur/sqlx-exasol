use std::{num::NonZeroUsize, ops::Not};

use serde::{de::Error, Deserialize, Deserializer, Serialize};

/// Struct representing attributes related to the connection with the Exasol server.
/// These can either be returned by an explicit `getAttributes` call or as part of any response.
///
/// Note that some of these are *read-only*!
/// See the [specification](<https://github.com/exasol/websocket-api/blob/master/docs/WebsocketAPIV1.md#attributes-session-and-database-properties>)
/// for more details.
///
/// Moreover, we store in this other custom connection related attributes, specific to the driver.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
#[allow(clippy::struct_excessive_bools)]
pub struct ExaAttributes {
    // ##########################################################
    // ############# Database read-write attributes #############
    // ##########################################################
    autocommit: bool,
    // The API doesn't having no schema open through this attribute,
    // hence the serialization skip.
    //
    // It is possible to change it through the attribute though.
    #[serde(skip_serializing_if = "Option::is_none")]
    current_schema: Option<String>,
    feedback_interval: u64,
    numeric_characters: String,
    query_timeout: u64,
    snapshot_transactions_enabled: bool,
    timestamp_utc_enabled: bool,
    // ##########################################################
    // ############# Database read-only attributes ##############
    // ##########################################################
    #[serde(skip_serializing)]
    compression_enabled: bool,
    #[serde(skip_serializing)]
    date_format: String,
    #[serde(skip_serializing)]
    date_language: String,
    #[serde(skip_serializing)]
    datetime_format: String,
    #[serde(skip_serializing)]
    default_like_escape_character: String,
    #[serde(skip_serializing)]
    open_transaction: bool,
    #[serde(skip_serializing)]
    timezone: String,
    #[serde(skip_serializing)]
    timezone_behavior: String,
    // ##########################################################
    // ############# Driver specific attributes #################
    // ##########################################################
    #[serde(skip_serializing)]
    fetch_size: usize,
    #[serde(skip_serializing)]
    encryption_enabled: bool,
    #[serde(skip_serializing)]
    statement_cache_capacity: NonZeroUsize,
}

impl ExaAttributes {
    pub(crate) fn new(
        compression_enabled: bool,
        fetch_size: usize,
        encryption_enabled: bool,
        statement_cache_capacity: NonZeroUsize,
    ) -> Self {
        Self {
            autocommit: true,
            current_schema: None,
            feedback_interval: 1,
            numeric_characters: ".,".to_owned(),
            query_timeout: 0,
            snapshot_transactions_enabled: false,
            timestamp_utc_enabled: false,
            compression_enabled,
            date_format: "YYYY-MM-DD".to_owned(),
            date_language: "ENG".to_owned(),
            datetime_format: "YYYY-MM-DD HH24:MI:SS.FF6".to_owned(),
            default_like_escape_character: "\\".to_owned(),
            open_transaction: false,
            timezone: "UNIVERSAL".to_owned(),
            timezone_behavior: "INVALID SHIFT AMBIGUOUS ST".to_owned(),
            fetch_size,
            encryption_enabled,
            statement_cache_capacity,
        }
    }
    #[must_use]
    pub fn autocommit(&self) -> bool {
        self.autocommit
    }

    pub fn set_autocommit(&mut self, autocommit: bool) -> &mut Self {
        self.autocommit = autocommit;
        self
    }

    #[must_use]
    pub fn current_schema(&self) -> Option<&str> {
        self.current_schema.as_deref()
    }

    /// Note that setting the open schema to `None` cannot be done
    /// through this attribute. It can only be changed to a different one.
    ///
    /// An explicit `CLOSE SCHEMA;` statement would have to be executed
    /// to accomplish the `no open schema` behavior.
    pub fn set_current_schema(&mut self, schema: String) -> &mut Self {
        self.current_schema = Some(schema);
        self
    }

    #[must_use]
    pub fn feedback_interval(&self) -> u64 {
        self.feedback_interval
    }

    pub fn set_feedback_interval(&mut self, feedback_interval: u64) -> &mut Self {
        self.feedback_interval = feedback_interval;
        self
    }

    #[must_use]
    pub fn numeric_characters(&self) -> &str {
        &self.numeric_characters
    }

    pub fn set_numeric_characters(&mut self, numeric_characters: String) -> &mut Self {
        self.numeric_characters = numeric_characters;
        self
    }

    #[must_use]
    pub fn query_timeout(&self) -> u64 {
        self.query_timeout
    }

    #[must_use]
    pub fn set_query_timeout(&mut self, query_timeout: u64) -> &mut Self {
        self.query_timeout = query_timeout;
        self
    }

    #[must_use]
    pub fn snapshot_transactions_enabled(&self) -> bool {
        self.snapshot_transactions_enabled
    }

    pub fn set_snapshot_transactions_enabled(&mut self, enabled: bool) -> &mut Self {
        self.snapshot_transactions_enabled = enabled;
        self
    }

    #[must_use]
    pub fn timestamp_utc_enabled(&self) -> bool {
        self.timestamp_utc_enabled
    }

    pub fn set_timestamp_utc_enabled(&mut self, enabled: bool) -> &mut Self {
        self.timestamp_utc_enabled = enabled;
        self
    }

    #[must_use]
    pub fn compression_enabled(&self) -> bool {
        self.compression_enabled
    }

    #[must_use]
    pub fn date_format(&self) -> &str {
        &self.date_format
    }

    #[must_use]
    pub fn date_language(&self) -> &str {
        &self.date_language
    }

    #[must_use]
    pub fn datetime_format(&self) -> &str {
        &self.datetime_format
    }

    #[must_use]
    pub fn default_like_escape_character(&self) -> &str {
        &self.default_like_escape_character
    }

    #[must_use]
    pub fn open_transaction(&self) -> bool {
        self.open_transaction
    }

    #[must_use]
    pub fn timezone(&self) -> &str {
        &self.timezone
    }

    #[must_use]
    pub fn timezone_behavior(&self) -> &str {
        &self.timezone_behavior
    }

    #[must_use]
    pub fn fetch_size(&self) -> usize {
        self.fetch_size
    }

    pub fn set_fetch_size(&mut self, fetch_size: usize) -> &mut Self {
        self.fetch_size = fetch_size;
        self
    }

    #[must_use]
    pub fn encryption_enabled(&self) -> bool {
        self.encryption_enabled
    }

    #[must_use]
    pub fn statement_cache_capacity(&self) -> NonZeroUsize {
        self.statement_cache_capacity
    }

    pub(crate) fn update(&mut self, other: Attributes) {
        macro_rules! other_or_prev {
            ($field:tt) => {
                if let Some(new) = other.$field {
                    self.$field = new;
                }
            };
        }

        self.current_schema = other.current_schema;

        other_or_prev!(autocommit);
        other_or_prev!(feedback_interval);
        other_or_prev!(numeric_characters);
        other_or_prev!(query_timeout);
        other_or_prev!(snapshot_transactions_enabled);
        other_or_prev!(timestamp_utc_enabled);
        other_or_prev!(compression_enabled);
        other_or_prev!(date_format);
        other_or_prev!(date_language);
        other_or_prev!(datetime_format);
        other_or_prev!(default_like_escape_character);
        other_or_prev!(open_transaction);
        other_or_prev!(timezone);
        other_or_prev!(timezone_behavior);
    }
}

/// Helper type representing only the attributes returned from Exasol.
///
/// While [`ExaAttributes`] are stored by connections and sent to
/// the database, this type is used for deserialization of the attributes
/// that Exasol sends to us.
///
/// The returned attributes are then used to update a connection's [`ExaAttributes`].
//
// Exasol can return virtually any combination of these attributes and
// accommodating that into [`ExaAttributes`] would be more convoluted than
// having this as a helper type.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Attributes {
    // ##########################################################
    // ############# Database read-write attributes #############
    // ##########################################################
    pub autocommit: Option<bool>,
    #[serde(default)]
    #[serde(deserialize_with = "Attributes::deserialize_current_schema")]
    pub current_schema: Option<String>,
    pub feedback_interval: Option<u64>,
    pub numeric_characters: Option<String>,
    pub query_timeout: Option<u64>,
    pub snapshot_transactions_enabled: Option<bool>,
    pub timestamp_utc_enabled: Option<bool>,
    // ##########################################################
    // ############# Database read-only attributes ##############
    // ##########################################################
    pub compression_enabled: Option<bool>,
    pub date_format: Option<String>,
    pub date_language: Option<String>,
    pub datetime_format: Option<String>,
    pub default_like_escape_character: Option<String>,
    #[serde(default)]
    #[serde(deserialize_with = "Attributes::deserialize_open_transaction")]
    pub open_transaction: Option<bool>,
    pub timezone: Option<String>,
    pub timezone_behavior: Option<String>,
}

impl Attributes {
    /// For some reason Exasol returns this as a number, although it's listed as bool.
    fn deserialize_open_transaction<'de, D>(deserializer: D) -> Result<Option<bool>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let Some(value) = Option::deserialize(deserializer)? else {
            return Ok(None);
        };

        match value {
            0 => Ok(Some(false)),
            1 => Ok(Some(true)),
            v => Err(D::Error::custom(format!(
                "Invalid value for 'open_transaction' field: {v}"
            ))),
        }
    }

    /// Exasol returns an empty string if no schema was selected.
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
