use std::{
    ops::{Index, IndexMut},
    sync::Arc,
};

use bson::RawDocument;
use bumpalo::{Bump, collections::Vec};
use serde::{
    Deserializer, Serialize,
    de::{DeserializeSeed, Visitor},
    ser::SerializeMap,
};

use crate::sync_plan::{
    CompatibilityContext, EvaluationError, HydratedSyncConfig, ReplicationEmbedder, SqliteValue,
    StringInterner, Symbol,
    expression::TableValuedFunction,
    operators::table_valued::TableValuedFunctionEvaluation,
    processor_group::{
        EvaluatorInstruction, PreparedProjection, ProcessedGroupMember, ProcessorGroup,
        ScalarOperation, Slot,
    },
    sources::{
        bson::SqliteValueFromBson,
        json::{ScalarSqliteValueOrText, SqliteValueFromJson},
    },
    table_pattern::SourceTableRef,
};

/// Evaluates rows to buckets, parameter results, and event notifications.
///
/// Evaluators are meant to be thread-local, while the [HydratedSyncConfig] contains prepared state
/// and can be used across threads.
pub struct Evaluator<E: ReplicationEmbedder> {
    config: Arc<HydratedSyncConfig<E>>,
    /// Storage for values computed while evaluating a row, reset for each row.
    arena: Bump,
}

impl<E: ReplicationEmbedder> Evaluator<E> {
    pub fn new(config: Arc<HydratedSyncConfig<E>>) -> Self {
        Self {
            config,
            arena: Bump::new(),
        }
    }

    /// Starts evaluating a row in the given source table.
    ///
    /// Returns [None] if no processor matches the table, in which case the row doesn't need to be
    /// decoded at all. Otherwise, call [RowEvaluation::prepare_field] for each field in the source
    /// row and then [RowEvaluation::finish].
    pub fn evaluate<'a>(&'a mut self, table: SourceTableRef<'a>) -> Option<RowEvaluation<'a, E>> {
        // Values of the previous row are no longer referenced, since the buffers are empty.
        self.arena.reset();

        let config: &'a HydratedSyncConfig<E> = &self.config;
        let arena = &self.arena;
        let group = config.groups.lookup(table)?;

        Some(RowEvaluation {
            config,
            group,
            table,
            frame: Frame::new(
                arena,
                &config.compatibility,
                group.num_input_slots,
                group.num_internal_slots,
            ),
        })
    }
}

/// A row being evaluated.
///
/// `'a` refers both to values allocated in the scoped bumpalo allocator and to values borrowed from
/// the processed row.
pub struct RowEvaluation<'a, E: ReplicationEmbedder> {
    config: &'a HydratedSyncConfig<E>,
    group: &'a ProcessorGroup<E>,
    table: SourceTableRef<'a>,
    frame: Frame<'a>,
}

impl<'a, E: ReplicationEmbedder> RowEvaluation<'a, E> {
    /// Prepares a field of the source row.
    ///
    /// Returns [None] if the field is not used by any processor, so that it doesn't have to be
    /// decoded. Otherwise, the value should be set with [PreparedField::set] or
    /// [PreparedField::set_converted].
    pub fn prepare_field(&mut self, name: &'a str) -> Option<PreparedField<'_, 'a>> {
        let symbol = self.config.interner.get(name);
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
    pub fn push_bson(&mut self, bson: &'a [u8]) -> Result<(), bson::error::Error> {
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

        Ok(())
    }

    /// Reads a row through the serde deserializer.
    ///
    /// This is designed for JSON, other formats will likely error.
    pub fn push_json<D: Deserializer<'a>>(&mut self, deserializer: D) -> Result<(), D::Error> {
        struct JsonKeyVisitor<'a> {
            alloc: &'a Bump,
        }

        struct ProcessRecord<'a, 'b, E: ReplicationEmbedder>(&'b mut RowEvaluation<'a, E>);

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
                        continue;
                    };

                    let parsed_value = map.next_value_seed(ScalarSqliteValueOrText(
                        SqliteValueFromJson::from(&*field.frame),
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

        deserializer.deserialize_map(ProcessRecord(self))
    }

    /// Evaluates all processors matching the row, reporting results to the `sink`.
    pub fn finish(self, sink: &mut impl EvaluationSink<E>) {
        let Self {
            config,
            group,
            table,
            mut frame,
        } = self;

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

        for member in &group.processors {
            let (id, result) = match member {
                ProcessedGroupMember::Event(id) => {
                    sink.event(id);
                    continue;
                }
                ProcessedGroupMember::Data(source) => {
                    let output_table = source
                        .output_table_name
                        .as_ref()
                        .map(|s| resolve(&config.interner, *s))
                        .unwrap_or(table.name);
                    let projection = &source.projection;
                    let mut parameters = frame.allocate_value_slice(&source.common.parameters);

                    let result = frame.run(&source.common.instructions, &mut |frame| {
                        let row = ProjectedRow {
                            interner: &config.interner,
                            projection,
                            frame,
                            star_columns: if projection.columns.has_star() {
                                &frame.star_columns
                            } else {
                                &[]
                            },
                        };
                        let parameters = frame.read_values(&mut parameters);
                        sink.row(&source.id, parameters, output_table, &row);
                    });
                    (ErrorSource::Data(source.id.as_ref()), result)
                }
                ProcessedGroupMember::Parameter(source) => {
                    let mut lookup = frame.allocate_value_slice(&source.common.parameters);
                    let mut output = frame.allocate_value_slice(&source.outputs);

                    let result = frame.run(&source.common.instructions, &mut |frame| {
                        let lookup = &frame.read_values(&mut lookup);
                        let outputs = &frame.read_values(&mut output);
                        sink.parameter_lookup(&source.id, lookup, outputs);
                    });
                    (ErrorSource::Parameter(source.id.as_ref()), result)
                }
            };

            if let Err(e) = result {
                sink.error(id, e);
            }

            // Clear internal values. This technically isn't necessary since processors shouldn't
            // read values they haven't written, but playing it safe doesn't cost much.
            frame.internals.fill(SqliteValue::Null);
        }
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

/// A field of the source row included in `*` projections.
#[derive(Clone, Copy, Debug)]
pub struct StarColumn<'a> {
    pub name: &'a str,
    /// The interned name, or [None] if no processor references a column with that name.
    symbol: Option<Symbol>,
    pub value: SqliteValue<'a>,
}

/// Receives results of evaluating a row.
pub trait EvaluationSink<E: ReplicationEmbedder> {
    /// Called for each bucket the row is put into.
    fn row(
        &mut self,
        bucket: &E::BucketId,
        parameters: &[SqliteValue<'_>],
        table: &str,
        row: &ProjectedRow<'_, '_>,
    );

    /// Called for each parameter lookup created from the row.
    fn parameter_lookup(
        &mut self,
        id: &E::ParameterId,
        lookup: &[SqliteValue<'_>],
        outputs: &[SqliteValue<'_>],
    );

    /// Called for each event that the row's table is relevant for.
    fn event(&mut self, id: &E::EventId);

    fn error(&mut self, source: ErrorSource<E>, error: EvaluationError);
}

pub enum ErrorSource<'a, E: ReplicationEmbedder> {
    Data(&'a E::BucketId),
    Parameter(&'a E::ParameterId),
}

/// An output row of a data source.
pub struct ProjectedRow<'r, 'a> {
    interner: &'a StringInterner,
    projection: &'a PreparedProjection,
    /// The frame of the evaluated row, storing values referenced by
    /// [crate::sync_plan::processor_group::PreparedColumn::slot].
    frame: &'r Frame<'a>,
    star_columns: &'r [StarColumn<'a>],
}

impl Serialize for ProjectedRow<'_, '_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let Self {
            interner,
            projection,
            frame,
            star_columns,
        } = self;
        let mut map = serializer.serialize_map(None)?;

        let serialize_value = &mut |key: &str, value: &SqliteValue| {
            if !value.is_json_compatible() {
                return Ok(());
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
                if projection.after_aliases.contains(&known_symbol) {
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

        map.end()
    }
}

fn resolve(interner: &StringInterner, symbol: Symbol) -> &str {
    interner
        .resolve(symbol)
        .expect("aliases are interned in the config's interner")
}

/// State for evaluating a single row.
pub struct Frame<'a> {
    arena: &'a Bump,
    compatibility: &'a CompatibilityContext,
    /// Values for [Slot::Input], written before evaluating processors.
    inputs: &'a mut [SqliteValue<'a>],
    /// Values for [Slot::Internal], written by instructions of the processor being evaluated.
    internals: &'a mut [SqliteValue<'a>],
    star_columns: Vec<'a, StarColumn<'a>>,
}

impl<'a> Frame<'a> {
    pub(crate) fn new(
        arena: &'a Bump,
        compatibility: &'a CompatibilityContext,
        num_inputs: u32,
        num_internals: u32,
    ) -> Self {
        Self {
            arena,
            compatibility,
            inputs: arena.alloc_slice_fill_default(num_inputs as usize),
            internals: arena.alloc_slice_fill_default(num_internals as usize),
            star_columns: Vec::new_in(arena),
        }
    }

    /// The allocator for values computed while evaluating this row.
    pub fn arena(&self) -> &'a Bump {
        self.arena
    }

    pub fn compatibility(&self) -> &'a CompatibilityContext {
        self.compatibility
    }

    /// Runs instructions of a processor, calling `on_match` for each combination of table-valued
    /// function rows for which all filters are truthy.
    fn run(
        &mut self,
        instructions: &'a [EvaluatorInstruction],
        on_match: &mut dyn FnMut(&Self),
    ) -> Result<(), EvaluationError<'a>> {
        for (index, instruction) in instructions.iter().enumerate() {
            match instruction {
                EvaluatorInstruction::Evaluate { op, output_slot } => {
                    self[*output_slot] = self.evaluate(op);
                }
                EvaluatorInstruction::CheckFilter(slot) => {
                    if !self[*slot].is_truthy() {
                        return Ok(());
                    }
                }
                EvaluatorInstruction::ExpandTableValuedFunction(function) => {
                    let mut table_valued = match function.function {
                        TableValuedFunction::JsonEach => TableValuedFunctionEvaluation::json_each(
                            self,
                            &function.inputs,
                            &function.outputs,
                        ),
                    }?;
                    let remaining = &instructions[index + 1..];

                    while table_valued.next(self) {
                        self.run(remaining, on_match)?;
                    }
                    return Ok(());
                }
            }
        }

        on_match(self);
        Ok(())
    }

    fn evaluate(&self, op: &'a ScalarOperation) -> SqliteValue<'a> {
        match op {
            ScalarOperation::Literal(literal) => literal.evaluate(),
            _ => todo!("Evaluate expression"),
        }
    }

    fn allocate_value_slice(&self, slots: &'a [Slot]) -> PendingValueSlice<'a> {
        PendingValueSlice {
            slots,
            buffer: self.arena.alloc_slice_fill_default(slots.len()),
        }
    }

    fn read_values<'b>(&self, slice: &'b mut PendingValueSlice<'a>) -> &'b [SqliteValue<'a>] {
        for (slot, value) in slice.slots.iter().zip(slice.buffer.iter_mut()) {
            *value = self[*slot];
        }

        slice.buffer
    }
}

impl<'a> Index<Slot> for Frame<'a> {
    type Output = SqliteValue<'a>;

    fn index(&self, index: Slot) -> &Self::Output {
        match index {
            Slot::Input(index) => &self.inputs[index as usize],
            Slot::Internal(index) => &self.internals[index as usize],
        }
    }
}

impl<'a> IndexMut<Slot> for Frame<'a> {
    fn index_mut(&mut self, index: Slot) -> &mut Self::Output {
        match index {
            Slot::Input(index) => &mut self.inputs[index as usize],
            Slot::Internal(index) => &mut self.internals[index as usize],
        }
    }
}

struct PendingValueSlice<'a> {
    slots: &'a [Slot],
    buffer: &'a mut [SqliteValue<'a>],
}

#[cfg(test)]
mod test {
    use std::{borrow::Cow, sync::Arc};

    use serde_json::{Value, json, to_value};

    use super::{EvaluationSink, Evaluator, ProjectedRow};
    use crate::sync_plan::{
        HydratedSource, SqliteValue,
        expression::SqlExpression,
        plan::{
            ColumnSource, RowProjection, StreamDataSource, StreamParameterIndexLookupCreator,
            TableProcessor, TableProcessorData, TableProcessorExpression,
        },
        test::{TestEmbedder, evaluator, event_source, source_table_ref, table_pattern},
    };

    #[derive(Debug, PartialEq)]
    struct RecordedRow {
        bucket: &'static str,
        parameters: Vec<String>,
        table: String,
        columns: Value,
    }

    #[derive(Default)]
    struct RecordingSink {
        rows: Vec<RecordedRow>,
        lookups: Vec<(&'static str, Vec<String>, Vec<String>)>,
        events: Vec<&'static str>,
    }

    fn format_values(values: &[SqliteValue<'_>]) -> Vec<String> {
        values.iter().map(|v| format!("{v:?}")).collect()
    }

    impl EvaluationSink<TestEmbedder> for RecordingSink {
        fn row(
            &mut self,
            bucket: &&'static str,
            parameters: &[SqliteValue<'_>],
            table: &str,
            row: &ProjectedRow<'_, '_>,
        ) {
            self.rows.push(RecordedRow {
                bucket,
                parameters: format_values(parameters),
                table: table.to_string(),
                columns: to_value(row).expect("serialize output row to json"),
            });
        }

        fn parameter_lookup(
            &mut self,
            id: &&'static str,
            lookup: &[SqliteValue<'_>],
            outputs: &[SqliteValue<'_>],
        ) {
            self.lookups
                .push((id, format_values(lookup), format_values(outputs)));
        }

        fn event(&mut self, id: &&'static str) {
            self.events.push(id);
        }

        fn error(
            &mut self,
            _source: super::ErrorSource<TestEmbedder>,
            error: crate::sync_plan::EvaluationError,
        ) {
            panic!("Unexpected error: {error}")
        }
    }

    fn column(name: &str) -> TableProcessorExpression {
        SqlExpression::ExternalData(TableProcessorData::ColumnInSourceRow(name.to_string()))
    }

    fn alias(alias: &str, expr: TableProcessorExpression) -> ColumnSource {
        ColumnSource {
            alias: alias.to_string(),
            expr,
        }
    }

    fn processor(table: &str) -> TableProcessor {
        TableProcessor {
            source_table: table_pattern(table),
            filters: vec![],
            parameters: vec![],
            table_valued_functions: vec![],
        }
    }

    fn data_source(table: &str, projection: RowProjection<ColumnSource>) -> StreamDataSource {
        StreamDataSource {
            processor: processor(table),
            projection,
            output_table_name: None,
        }
    }

    fn bucket(id: &'static str, source: StreamDataSource) -> HydratedSource<TestEmbedder> {
        HydratedSource::Bucket {
            id: Arc::new(id),
            source,
        }
    }

    /// A (name, value) pair representing a source row field.
    type Field<'a> = (&'a str, SqliteValue<'a>);

    /// Evaluates a row, returning the names of fields that were requested.
    fn evaluate_row<'a>(
        evaluator: &mut Evaluator<TestEmbedder>,
        table_name: &'a str,
        fields: &[Field<'a>],
        sink: &mut RecordingSink,
    ) -> Vec<&'a str> {
        let mut requested = vec![];
        let mut row = evaluator.evaluate(source_table_ref(table_name)).unwrap();
        for (name, value) in fields {
            if let Some(field) = row.prepare_field(&Cow::Borrowed(name)) {
                requested.push(*name);
                field.set(*value);
            }
        }

        row.finish(sink);
        requested
    }

    #[test]
    fn no_matching_group() {
        let source = data_source("users", RowProjection::star());
        let mut eval = evaluator(&[bucket("b", source)]);

        assert!(eval.evaluate(source_table_ref("accounts")).is_none());
    }

    #[test]
    fn skips_unused_fields() {
        let mut source = data_source(
            "users",
            RowProjection::Columns(vec![alias("a", column("a"))]),
        );
        source.processor.filters.push(column("b"));
        let mut evaluator = evaluator(&[bucket("b", source)]);
        let mut sink = RecordingSink::default();

        let requested = evaluate_row(
            &mut evaluator,
            "users",
            &[
                ("a", SqliteValue::Integer(1)),
                ("b", SqliteValue::Integer(1)),
                ("c", SqliteValue::Integer(1)),
            ],
            &mut sink,
        );
        assert_eq!(requested, ["a", "b"]);

        // The filter evaluates to false here.
        evaluate_row(
            &mut evaluator,
            "users",
            &[("a", SqliteValue::Integer(2))],
            &mut sink,
        );

        assert_eq!(
            sink.rows,
            [RecordedRow {
                bucket: "b",
                parameters: vec![],
                table: "users".to_string(),
                columns: json!({"a": 1}),
            }]
        );
    }

    #[test]
    fn projection_order() {
        let mut source = data_source(
            "users",
            RowProjection::Star {
                before: vec![alias("x", column("a"))],
                after: vec![],
            },
        );
        source.output_table_name = Some("renamed".to_string());
        let mut evaluator = evaluator(&[bucket("b", source)]);
        let mut sink = RecordingSink::default();

        evaluate_row(
            &mut evaluator,
            "users",
            &[("a", SqliteValue::Integer(1))],
            &mut sink,
        );

        assert_eq!(
            sink.rows,
            [RecordedRow {
                bucket: "b",
                parameters: vec![],
                table: "renamed".to_string(),
                columns: json!({"x":1, "a":1})
            }]
        );
    }

    #[test]
    fn emits_columns_once() {
        // SELECT row.a AS x, row.a AS y, *, row.b AS z FROM users
        let source = data_source(
            "users",
            RowProjection::Star {
                before: vec![alias("x", column("a")), alias("y", column("a"))],
                after: vec![alias("z", column("b"))],
            },
        );
        let mut evaluator = evaluator(&[bucket("b", source)]);
        let mut sink = RecordingSink::default();

        evaluate_row(
            &mut evaluator,
            "users",
            &[
                ("a", SqliteValue::Integer(1)),
                ("b", SqliteValue::Integer(2)),
                ("x", SqliteValue::Integer(3)),
                ("z", SqliteValue::Integer(4)),
            ],
            &mut sink,
        );

        // The x column is shadowed by the source row, the z field by the z column.
        assert_eq!(
            sink.rows[0].columns,
            json!({"y": 1, "a": 1, "b": 2, "x": 3, "z" :2})
        );

        // Without an x field in the source row, the column before the star is emitted.
        evaluate_row(
            &mut evaluator,
            "users",
            &[("a", SqliteValue::Integer(1))],
            &mut sink,
        );
        assert_eq!(
            sink.rows[1].columns,
            json!({"x": 1, "y": 1, "a": 1, "z":null})
        );
    }

    #[test]
    fn parameters_and_events() {
        let mut data = data_source("users", RowProjection::Columns(vec![]));
        data.processor.parameters.push(column("org"));

        let mut lookup_processor = processor("user%");
        lookup_processor.parameters = vec![
            column("org"),
            SqlExpression::ExternalData(TableProcessorData::SchemaName),
            SqlExpression::ExternalData(TableProcessorData::TableSuffix),
        ];
        let lookup = StreamParameterIndexLookupCreator {
            processor: lookup_processor,
            outputs: vec![column("id")],
        };

        let mut evaluator = evaluator(&[
            bucket("b", data),
            HydratedSource::Parameter {
                id: Arc::new("lookup"),
                source: lookup,
            },
            event_source("event", &["users", "user%"]),
        ]);
        let mut sink = RecordingSink::default();

        evaluate_row(
            &mut evaluator,
            "users",
            &[
                ("id", SqliteValue::Integer(1)),
                ("org", SqliteValue::Text("o")),
            ],
            &mut sink,
        );

        assert_eq!(sink.rows.len(), 1);
        assert_eq!(sink.rows[0].parameters, ["Text(\"o\")"]);
        assert_eq!(
            sink.lookups,
            [(
                "lookup",
                vec![
                    "Text(\"o\")".to_string(),
                    "Text(\"public\")".to_string(),
                    "Text(\"s\")".to_string()
                ],
                vec!["Integer(1)".to_string()]
            )]
        );
        assert_eq!(sink.events, ["event"]);
    }

    #[test]
    fn shares_symbols_between_groups() {
        let id = Arc::new("b");
        let evaluator = evaluator(&[
            HydratedSource::Bucket {
                id: id.clone(),
                source: data_source("%", RowProjection::Columns(vec![alias("id", column("id"))])),
            },
            HydratedSource::Bucket {
                id,
                source: data_source("users", RowProjection::Columns(vec![])),
            },
        ]);
        let config = &evaluator.config;
        let id = config.interner.get("id").unwrap();

        let users = config.groups.lookup(source_table_ref("users")).unwrap();
        let accounts = config.groups.lookup(source_table_ref("accounts")).unwrap();
        assert!(!Arc::ptr_eq(users, accounts));
        assert!(users.input_columns.contains_key(&id));
        assert!(accounts.input_columns.contains_key(&id));
    }
}
