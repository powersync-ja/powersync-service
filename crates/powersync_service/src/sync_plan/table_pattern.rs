use std::str::FromStr;

use serde::Deserialize;

/// Matches a table or schema name.
#[derive(Clone, Debug)]
pub struct NameMatcher {
    pub name: String,
    pub wildcard: bool,
}

impl NameMatcher {
    pub fn from_string(mut src: String) -> Self {
        let wildcard = if src.ends_with('%') {
            src.pop();
            true
        } else {
            false
        };

        Self {
            name: src,
            wildcard,
        }
    }
}

impl<'a> Deserialize<'a> for NameMatcher {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'a>,
    {
        let name: String = String::deserialize(deserializer)?;
        Ok(Self::from_string(name))
    }
}

impl FromStr for NameMatcher {
    type Err = !;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::from_string(s.to_owned()))
    }
}

#[derive(Clone, Debug)]
pub struct TablePattern {
    pub connection_tag: String,
    pub schema: NameMatcher,
    pub table: NameMatcher,
}

impl TablePattern {
    pub const DEFAULT_TAG: &'static str = "default";
}

/// Reference to a source table associated with a replicated row.
#[derive(Clone, Copy, Debug)]
pub struct SourceTableRef<'a> {
    /// A tag associated with the connection.
    ///
    /// This is not currently supported, the value must be `default`.
    pub connection_tag: &'a str,
    /// The name of the schema.
    pub schema: &'a str,
    /// The name of the table.
    pub name: &'a str,
}

#[cfg(test)]
mod test {

    use super::NameMatcher;

    #[test]
    fn parse() {
        let Ok(matcher) = "users".parse::<NameMatcher>();
        assert_eq!(matcher.name, "users");
        assert!(!matcher.wildcard);

        let Ok(matcher) = "users%".parse::<NameMatcher>();
        assert_eq!(matcher.name, "users");
        assert!(matcher.wildcard);
    }
}
