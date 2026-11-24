use serde::{Serialize, ser::SerializeMap};

use crate::sync_plan::{
    SqliteValue, StringInterner,
    evaluation::{Frame, frame::StarColumn, resolve},
    processor_group::PreparedProjection,
};

/// An output row of a data source, serializing `SELECT ...` columns of a Sync Stream to JSON.
pub struct ProjectedRow<'r, 'a> {
    pub(super) interner: &'a StringInterner,
    pub(super) projection: &'a PreparedProjection,
    /// The frame of the evaluated row, storing values referenced by
    /// [crate::sync_plan::processor_group::PreparedColumn::slot].
    pub(super) frame: &'r Frame<'a>,
    pub(super) star_columns: &'r [StarColumn<'a>],
}

impl<'a> ProjectedRow<'_, 'a> {
    /// Serializes this row, also returns the value for the `id` column.
    pub fn serialize_and_extract_id<S>(
        &self,
        serializer: S,
    ) -> Result<(S::Ok, SqliteValue<'a>), S::Error>
    where
        S: serde::Serializer,
    {
        let mut id = SqliteValue::Null {
            is_undefined: false,
        };
        let Self {
            interner,
            projection,
            frame,
            star_columns,
        } = self;
        let mut map = serializer.serialize_map(None)?;

        let serialize_value = &mut |key: &str, value: &SqliteValue<'a>| {
            if !value.is_json_compatible() {
                return Ok(());
            }

            if key == "id" {
                id = *value;
            }

            map.serialize_entry(key, &value)
        };

        for before in projection.columns.before() {
            // Skip this column if it's shadowed by the *. Values before the * are pretty rare, so
            // a linear scan is fine.
            if star_columns.iter().any(|c| c.symbol == Some(before.alias)) {
                continue; // shadowed by *
            }

            let key = resolve(interner, before.alias);
            serialize_value(key, &frame[before.slot])?;
        }

        for star_column in *star_columns {
            let value = star_column.value;
            let mut name = star_column.name;

            if let Some(known_symbol) = star_column.symbol {
                let is_shadowed = projection
                    .after_aliases
                    .get(&known_symbol)
                    .map(|slot| frame[*slot])
                    .is_some_and(|v| v.is_json_compatible());

                if is_shadowed {
                    // Shadowed by a value after the *, e.g. id in `SELECT *, ... as id`.
                    continue;
                }

                name = resolve(interner, known_symbol);
            }

            serialize_value(name, &value)?;
        }

        for after in projection.columns.after() {
            let key = resolve(interner, after.alias);
            serialize_value(key, &frame[after.slot])?;
        }

        Ok((map.end()?, id))
    }
}

impl Serialize for ProjectedRow<'_, '_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let (result, _) = self.serialize_and_extract_id(serializer)?;
        Ok(result)
    }
}
