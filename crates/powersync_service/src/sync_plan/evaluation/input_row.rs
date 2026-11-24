use std::mem::MaybeUninit;

use bson::RawDocument;
use bumpalo::Bump;
use serde::{
    Deserializer,
    de::{DeserializeSeed, IgnoredAny, Visitor},
};

use crate::sync_plan::{
    ReplicationEmbedder, SourceTableRef, SqliteValue, Symbol,
    evaluation::{Frame, RowEvaluation, frame::StarColumn, program::Program},
    processor_group::{ProcessorGroup, Slot},
    sources::{
        bson::{BsonSourceError, SqliteValueFromBson},
        json::{ScalarSqliteValueOrText, SqliteValueFromJson},
    },
};

/// Decodes an input row into the input slots described by a [ProcessorGroup].
///
/// Once all fields have been set, [Self::finish] returns a [RowEvaluation] to evaluate processors.
pub struct InputRow<'a, E: ReplicationEmbedder> {
    group: &'a ProcessorGroup<E>,
    table: SourceTableRef<'a>,
    frame: Frame<'a>,
}

impl<'a, E: ReplicationEmbedder> InputRow<'a, E> {
    pub fn new(
        group: &'a ProcessorGroup<E>,
        table: SourceTableRef<'a>,
        mut frame: Frame<'a>,
    ) -> Self {
        if let Some(slot) = group.input_schema_name {
            frame[slot] = SqliteValue::Text(table.schema);
        }
        if let Some(slot) = group.input_table_name {
            frame[slot] = SqliteValue::Text(table.name);
        }
        for suffix in &group.inputs_table_suffix {
            // The table matched a pattern of this length, so this is a valid char boundary.
            frame[suffix.slot] = SqliteValue::Text(&table.name[suffix.prefix_length..]);
        }

        Self {
            group,
            table,
            frame,
        }
    }

    /// The allocator for values of the row being evaluated.
    ///
    /// This can be used to allocate field names and values passed to [Self::prepare_field].
    pub fn arena(&self) -> &'a Bump {
        self.frame.arena
    }

    /// Prepares a field of the source row.
    ///
    /// Returns [None] if the field is not used by any processor, so that it doesn't have to be
    /// decoded. Otherwise, the value should be set with [PreparedField::set].
    pub fn prepare_field(&mut self, name: &'a str) -> Option<PreparedField<'_, 'a>> {
        let symbol = self.group.interner.get(name);
        let slot = symbol.and_then(|symbol| self.group.input_columns.get(&symbol).copied());
        let name_for_star = if self.group.has_star_projection {
            Some(name)
        } else {
            None
        };

        if slot.is_none() && name_for_star.is_none() {
            return None;
        }

        Some(PreparedField {
            frame: &mut self.frame,
            name_for_star,
            symbol,
            slot,
        })
    }

    /// Push a BSON source row into the evaluator.
    ///
    /// This iterates through and decodes BSON bytes where needed.
    pub fn push_bson(mut self, bson: &'a [u8]) -> Result<RowEvaluation<'a, E>, BsonSourceError> {
        let bson = RawDocument::from_bytes(bson)?;
        let from_bson = SqliteValueFromBson::from(&self.frame);

        for element in bson.iter_elements() {
            let element = element?;
            let Some(field) = self.prepare_field(element.key().as_str()) else {
                // No `*` column, and no reference to this field, ignore it.
                continue;
            };

            let parsed_value = from_bson.parse_top_level_value(element.value()?)?;
            field.set(parsed_value);
        }

        Ok(self.finish())
    }

    /// Reads a row through the serde deserializer.
    ///
    /// This is designed for JSON, other formats will likely error.
    pub fn push_json<D: Deserializer<'a>>(
        mut self,
        deserializer: D,
    ) -> Result<RowEvaluation<'a, E>, D::Error> {
        struct JsonKeyVisitor<'a> {
            alloc: &'a Bump,
        }

        struct ProcessRecord<'a, 'b, E: ReplicationEmbedder>(&'b mut InputRow<'a, E>);

        impl<'a, 'b, E: ReplicationEmbedder> Visitor<'a> for ProcessRecord<'a, 'b, E> {
            type Value = ();

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(formatter, "a JSON object")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'a>,
            {
                while let Some(key) = map.next_key_seed(JsonKeyVisitor {
                    alloc: self.0.frame.arena,
                })? {
                    let Some(field) = self.0.prepare_field(key) else {
                        // No `*` column, and no reference to this field, ignore it.
                        map.next_value::<IgnoredAny>()?;
                        continue;
                    };

                    let parsed_value = map.next_value_seed(ScalarSqliteValueOrText(
                        SqliteValueFromJson::for_source_row(field.frame),
                    ))?;
                    field.set(parsed_value);
                }

                Ok(())
            }
        }

        impl<'a> DeserializeSeed<'a> for JsonKeyVisitor<'a> {
            type Value = &'a str;

            fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: Deserializer<'a>,
            {
                deserializer.deserialize_str(self)
            }
        }

        impl<'a> Visitor<'a> for JsonKeyVisitor<'a> {
            type Value = &'a str;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(formatter, "a JSON key")
            }

            fn visit_borrowed_str<E>(self, v: &'a str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(v)
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(self.alloc.alloc_str(v))
            }
        }

        deserializer.deserialize_map(ProcessRecord(&mut self))?;
        Ok(self.finish())
    }

    /// Marks the input row as fully decoded, allowing data and parameter evaluators to operate on
    /// it.
    pub fn finish(self) -> RowEvaluation<'a, E> {
        let arena = self.frame.arena;
        RowEvaluation::new(
            self.group,
            self.table,
            Program::new(self.frame, self.group.max_table_valued_functions),
            arena.alloc_slice_fill_copy(self.group.max_parameters, MaybeUninit::uninit()),
        )
    }
}

/// A field of the source row that is used by at least one processor.
pub struct PreparedField<'r, 'a> {
    frame: &'r mut Frame<'a>,
    name_for_star: Option<&'a str>,
    symbol: Option<Symbol>,
    slot: Option<Slot>,
}

impl<'a> PreparedField<'_, 'a> {
    pub fn set(self, value: SqliteValue<'a>) {
        let frame = self.frame;
        if let Some(slot) = self.slot {
            frame[slot] = value;
        }

        if let Some(name) = self.name_for_star
            && value.is_json_compatible()
        {
            frame.star_columns.push(StarColumn {
                name,
                symbol: self.symbol,
                value,
            });
        }
    }
}
