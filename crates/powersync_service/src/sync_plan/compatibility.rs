use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct CompatibilityContext {
    pub max_time_value_precision: Option<TimeValuePrecision>,
    pub timestamps_iso8601: bool,
    pub fixed_json_extract: bool,
    pub custom_postgres_types: bool,
    pub fixed_booleans_in_json: bool,
}

#[derive(Clone, Copy, Debug, Deserialize)]
pub enum TimeValuePrecision {
    #[serde(rename = "seconds")]
    Seconds,
    #[serde(rename = "milliseconds")]
    Milliseconds,
    #[serde(rename = "microseconds")]
    Microseconds,
    #[serde(rename = "nanoseconds")]
    Nanoseconds,
}

impl CompatibilityContext {
    #[cfg(test)]
    pub fn default_for_tests() -> Self {
        Self {
            max_time_value_precision: None,
            timestamps_iso8601: true,
            fixed_json_extract: true,
            custom_postgres_types: true,
            fixed_booleans_in_json: true,
        }
    }
}
