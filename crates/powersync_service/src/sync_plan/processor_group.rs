use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use crate::sync_plan::{
    StringInterner, Symbol,
    expression::{
        BinaryOperator, CastType, LiteralExpression, ScalarFunction, SqlExpression,
        TableValuedFunction, UnaryOperator,
    },
    hydrated::ReplicationEmbedder,
    plan::{
        RowProjection, StreamDataSource, StreamParameterIndexLookupCreator, TableProcessor,
        TableProcessorData, TableProcessorExpression,
    },
};

/// A prepared group of processors all operating on compatible table patterns.
///
/// We build one group per unique combination of matched table patterns. In a Sync Config defining
/// streams selecting from tables `%`, `user%` and `users` for example, we'd end up with three
/// processor groups:
///
///  1. One containing all three processors, used for tables named `users`.
///  2. One containing `%` and `user%`, used for tables names beginning with `user` but not `users`.
///  3. One containing only `%`, used for table names not beginning with `user`.
///
/// In each group, processors can share decoded values. For example, if one stream has a
/// `SELECT * FROM users WHERE id = ...` and another has `SELECT id FROM users WHERE ...`, we'd
/// share a slot for the decoded `users.id` column instead of extracing them multiple times.
#[derive(Debug)]
pub struct ProcessorGroup<E: ReplicationEmbedder> {
    pub processors: Vec<ProcessedGroupMember<E>>,

    /// The maximum amount of [Slot::Input] slots used by a processor.
    pub num_input_slots: u32,
    /// The maximum amount of [Slot::Internal] slots used by any processor.
    ///
    /// Since processors are evaluated one after another and only read internal slots they have
    /// written before, internal slots are shared between processors. This is the maximum amount
    /// needed by any processor of the group.
    pub num_internal_slots: u32,

    /// For each column name used as input by any processor, the input slot to store it in when
    /// decoding a value.
    pub input_columns: HashMap<Symbol, Slot>,

    /// The input slot in which to write the schema name of the replicated row.
    pub input_schema_name: Option<Slot>,
    pub inputs_table_suffix: Vec<TableSuffixInput>,

    /// Whether any processor in this group has a `*` projection.
    pub has_star_projection: bool,
}

/// A reference to a value used while evaluating a [ProcessorGroup].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Slot {
    /// A value derived from the source row before evaluating processors, like a column value or the
    /// name of the table.
    Input(u32),
    /// An intermediate value written by an instruction of the processor being evaluated.
    Internal(u32),
}

#[derive(Debug)]
pub enum ProcessedGroupMember<E: ReplicationEmbedder> {
    Data(ProcessedDataSource<E>),
    Parameter(ProcessedParameterSource<E>),

    /// An event source.
    ///
    /// We don't need to evaluate the event here, only the fact that an event might be matched by
    /// the row to process.
    Event(Arc<E::EventId>),
}

#[derive(Debug)]
pub struct ProcessedDataSource<E: ReplicationEmbedder> {
    pub id: Arc<E::BucketId>,
    pub projection: PreparedProjection,
    /// The name of the output table for evaluated rows.
    ///
    /// When [None], the name of the table being evaluated should be used instead.
    pub output_table_name: Option<Arc<String>>,
    pub common: CommonProcessedSource,
}

/// A non-`*` output column of a [ProcessedDataSource].
#[derive(Debug)]
pub struct PreparedColumn {
    pub alias: Symbol,
    /// The slot storing the value of this column once all instructions have run.
    pub slot: Slot,
}

/// A [RowProjection] with an index of shadowed source row fields, used to emit every output
/// column at most once.
///
/// Since explicit columns have unique aliases, a column can only be shadowed by a field of the
/// source row or vice-versa:
///
///  1. Fields of the source row with the same name as a column after the `*` are not emitted.
///     These are known statically and stored in [Self::after_aliases].
///  2. Columns before the `*` are not emitted if the source row has a field with the same name,
///     which is checked during evaluation.
#[derive(Debug)]
pub struct PreparedProjection {
    pub columns: RowProjection<PreparedColumn>,
    /// Aliases of columns after the `*`, shadowing fields of the source row with the same name.
    pub after_aliases: HashSet<Symbol>,
}

impl PreparedProjection {
    pub fn new(columns: RowProjection<PreparedColumn>) -> Self {
        debug_assert!(
            {
                let explicit = columns.before().iter().chain(columns.after());
                let mut seen = HashSet::new();
                explicit.into_iter().all(|c| seen.insert(c.alias))
            },
            "explicit columns of a projection must have unique aliases"
        );

        Self {
            after_aliases: columns.after().iter().map(|c| c.alias).collect(),
            columns,
        }
    }
}

#[derive(Debug)]
pub struct ProcessedParameterSource<E: ReplicationEmbedder> {
    pub id: Arc<E::ParameterId>,
    /// The slots storing outputs to persist in the lookup.
    pub outputs: Vec<Slot>,
    pub common: CommonProcessedSource,
}

#[derive(Debug)]
pub struct CommonProcessedSource {
    /// Instructions to run for each row.
    ///
    /// After running all instructions without a filter failing, the slots referenced by
    /// [Self::parameters] and source-specific outputs contain the values for a match. Since
    /// [EvaluatorInstruction::ExpandTableValuedFunction] runs the remaining instructions once per
    /// row of the function, a source row can have multiple matches.
    pub instructions: Vec<EvaluatorInstruction>,
    /// The slots storing bucket parameters (for data sources) or lookup inputs (for parameter
    /// sources).
    pub parameters: Vec<Slot>,
}

#[derive(Debug)]
pub enum EvaluatorInstruction {
    /// Evaluate a scalar operation, writing its output into the given slot.
    Evaluate {
        op: ScalarOperation,
        output_slot: Slot,
    },

    /// Check that the given slot contains a truthy value, and stop processing the match otherwise.
    CheckFilter(Slot),

    /// Evaluates a table-valued function, running the remaining instructions once for each of its
    /// rows.
    ExpandTableValuedFunction(ProcessedTableValuedFunction),
}

/// A single, non-nested step of evaluating a [SqlExpression], with operands read from slots.
#[derive(Debug)]
pub enum ScalarOperation {
    Literal(LiteralExpression),
    Unary {
        operator: UnaryOperator,
        operand: Slot,
    },
    Binary {
        left: Slot,
        operator: BinaryOperator,
        right: Slot,
    },
    Between {
        value: Slot,
        low: Slot,
        high: Slot,
    },
    ScalarIn {
        target: Slot,
        r#in: Vec<Slot>,
    },
    CaseWhen {
        operand: Option<Slot>,
        /// Slots of `WHEN` and `THEN` values.
        whens: Vec<(Slot, Slot)>,
        r#else: Option<Slot>,
    },
    Cast {
        operand: Slot,
        cast_as: CastType,
    },
    FunctionCall {
        function: ScalarFunction,
        inputs: Vec<Slot>,
    },
}

#[derive(Debug)]
pub struct ProcessedTableValuedFunction {
    pub function: TableValuedFunction,
    pub inputs: Vec<Slot>,
    pub outputs: Vec<Slot>,
}

#[derive(Debug)]
pub struct TableSuffixInput {
    pub prefix_length: usize,
    pub slot: Slot,
}

impl<E: ReplicationEmbedder> Default for ProcessorGroup<E> {
    // Not derived, since that would require E: Default.
    fn default() -> Self {
        Self {
            processors: Vec::new(),
            num_input_slots: 0,
            num_internal_slots: 0,
            input_columns: HashMap::new(),
            input_schema_name: None,
            inputs_table_suffix: Vec::new(),
            has_star_projection: false,
        }
    }
}

impl<E: ReplicationEmbedder> ProcessorGroup<E> {
    pub fn include_data(
        &mut self,
        interner: &mut StringInterner,
        id: Arc<E::BucketId>,
        source: &StreamDataSource,
    ) {
        let mut translator = ProcessorTranslator::new(self, interner, &source.processor);
        let parameters = translator.translate_common();
        let projection = PreparedProjection::new(source.projection.map(|column| PreparedColumn {
            alias: translator.interner.get_or_intern(column.alias.as_str()),
            slot: translator.lower(&column.expr).slot,
        }));
        let common = translator.finish(parameters);
        self.has_star_projection = self.has_star_projection || projection.columns.has_star();

        self.processors
            .push(ProcessedGroupMember::Data(ProcessedDataSource {
                id,
                projection,
                output_table_name: source.output_table_name.clone(),
                common,
            }));
    }

    pub fn include_parameter(
        &mut self,
        interner: &mut StringInterner,
        id: Arc<E::ParameterId>,
        source: &StreamParameterIndexLookupCreator,
    ) {
        let mut translator = ProcessorTranslator::new(self, interner, &source.processor);
        let parameters = translator.translate_common();
        let outputs = translator.lower_all(&source.outputs, &mut 0);
        let common = translator.finish(parameters);

        self.processors
            .push(ProcessedGroupMember::Parameter(ProcessedParameterSource {
                id,
                outputs,
                common,
            }));
    }

    pub fn include_event(&mut self, id: Arc<E::EventId>) {
        self.processors.push(ProcessedGroupMember::Event(id));
    }
}

/// Allocates a new value slot, given a mutable reference to a slot counter like
/// [ProcessorGroup::num_input_slots].
///
/// This takes the field instead of the group so that it can be called while other fields of the
/// group are borrowed.
fn next_slot(num_slots: &mut u32) -> u32 {
    let current = *num_slots;
    *num_slots += 1;
    current
}

/// A slot storing the value of an expression, along with the amount of table-valued functions
/// that need to be expanded before the value is available.
///
/// A depth of `0` means that the value only depends on the source row, a depth of `i + 1` means
/// that it depends on outputs of the `i`-th table-valued function (and potentially earlier ones).
#[derive(Clone, Copy)]
struct LoweredValue {
    slot: Slot,
    depth: usize,
}

/// Lowers expressions of a single [TableProcessor] into [EvaluatorInstruction]s operating on slots
/// of a [ProcessorGroup].
struct ProcessorTranslator<'a, E: ReplicationEmbedder> {
    group: &'a mut ProcessorGroup<E>,
    interner: &'a mut StringInterner,
    processor: &'a TableProcessor,
    /// For each entry in [TableProcessor::table_valued_functions], the slots assigned to its
    /// output columns.
    function_outputs: Vec<Vec<Slot>>,
    /// The amount of internal slots used by this processor so far.
    num_internal_slots: u32,
    /// Instructions to run, grouped by the depth (see [LoweredValue::depth]) of their inputs.
    ///
    /// This allows evaluating expressions (and in particular, filters) before expanding
    /// table-valued functions they don't depend on.
    sections: Vec<Vec<EvaluatorInstruction>>,
}

impl<'a, E: ReplicationEmbedder> ProcessorTranslator<'a, E> {
    fn new(
        group: &'a mut ProcessorGroup<E>,
        interner: &'a mut StringInterner,
        processor: &'a TableProcessor,
    ) -> Self {
        let mut num_internal_slots = 0;
        let function_outputs = processor
            .table_valued_functions
            .iter()
            .map(|f| {
                f.function
                    .output_columns()
                    .iter()
                    .map(|_| Slot::Internal(next_slot(&mut num_internal_slots)))
                    .collect()
            })
            .collect();

        Self {
            group,
            interner,
            processor,
            function_outputs,
            num_internal_slots,
            sections: (0..=processor.table_valued_functions.len())
                .map(|_| Vec::new())
                .collect(),
        }
    }

    /// Lowers table-valued function inputs and filters, returning the slots for parameters.
    fn translate_common(&mut self) -> Vec<Slot> {
        let processor = self.processor;

        for (index, function) in processor.table_valued_functions.iter().enumerate() {
            let mut depth = 0;
            let inputs = self.lower_all(&function.inputs, &mut depth);
            debug_assert!(
                depth <= index,
                "table-valued function inputs can only depend on earlier functions"
            );

            self.sections[index + 1].push(EvaluatorInstruction::ExpandTableValuedFunction(
                ProcessedTableValuedFunction {
                    function: function.function,
                    inputs,
                    outputs: self.function_outputs[index].clone(),
                },
            ));
        }

        for filter in &processor.filters {
            let value = self.lower(filter);
            self.sections[value.depth].push(EvaluatorInstruction::CheckFilter(value.slot));
        }

        self.lower_all(&processor.parameters, &mut 0)
    }

    /// Concatenates lowered instructions, so that every instruction runs after the table-valued
    /// functions it depends on have been expanded.
    fn finish(self, parameters: Vec<Slot>) -> CommonProcessedSource {
        let group = self.group;
        group.num_internal_slots = group.num_internal_slots.max(self.num_internal_slots);

        CommonProcessedSource {
            instructions: self.sections.into_iter().flatten().collect(),
            parameters,
        }
    }

    /// Lowers an expression, returning the slot storing its value.
    ///
    /// For references to external data, this is the slot storing that data and no instruction is
    /// emitted.
    fn lower(&mut self, expr: &TableProcessorExpression) -> LoweredValue {
        let mut depth = 0;

        let op = match expr {
            SqlExpression::ExternalData(data) => return self.slot_for(data),
            SqlExpression::Literal(literal) => ScalarOperation::Literal(literal.clone()),
            SqlExpression::Unary(unary) => ScalarOperation::Unary {
                operator: unary.operator,
                operand: self.lower_operand(&unary.operand, &mut depth),
            },
            SqlExpression::Binary(binary) => ScalarOperation::Binary {
                left: self.lower_operand(&binary.left, &mut depth),
                operator: binary.operator,
                right: self.lower_operand(&binary.right, &mut depth),
            },
            SqlExpression::Between(between) => ScalarOperation::Between {
                value: self.lower_operand(&between.value, &mut depth),
                low: self.lower_operand(&between.low, &mut depth),
                high: self.lower_operand(&between.high, &mut depth),
            },
            SqlExpression::ScalarIn(scalar_in) => ScalarOperation::ScalarIn {
                target: self.lower_operand(&scalar_in.target, &mut depth),
                r#in: self.lower_all(&scalar_in.r#in, &mut depth),
            },
            SqlExpression::CaseWhen(case_when) => ScalarOperation::CaseWhen {
                operand: case_when
                    .operand
                    .as_ref()
                    .map(|e| self.lower_operand(e, &mut depth)),
                whens: case_when
                    .whens
                    .iter()
                    .map(|when_then| {
                        (
                            self.lower_operand(&when_then.when, &mut depth),
                            self.lower_operand(&when_then.then, &mut depth),
                        )
                    })
                    .collect(),
                r#else: case_when
                    .r#else
                    .as_ref()
                    .map(|e| self.lower_operand(e, &mut depth)),
            },
            SqlExpression::Cast(cast) => ScalarOperation::Cast {
                operand: self.lower_operand(&cast.operand, &mut depth),
                cast_as: cast.cast_as,
            },
            SqlExpression::FunctionCall(call) => ScalarOperation::FunctionCall {
                function: call.function,
                inputs: self.lower_all(&call.parameters, &mut depth),
            },
        };

        let output_slot = Slot::Internal(next_slot(&mut self.num_internal_slots));
        self.sections[depth].push(EvaluatorInstruction::Evaluate { op, output_slot });
        LoweredValue {
            slot: output_slot,
            depth,
        }
    }

    /// Lowers an operand of an expression, raising `depth` to the depth of the operand.
    fn lower_operand(&mut self, expr: &TableProcessorExpression, depth: &mut usize) -> Slot {
        let value = self.lower(expr);
        *depth = (*depth).max(value.depth);
        value.slot
    }

    /// Lowers multiple operands, see [Self::lower_operand].
    fn lower_all(
        &mut self,
        expressions: &[TableProcessorExpression],
        depth: &mut usize,
    ) -> Vec<Slot> {
        expressions
            .iter()
            .map(|expr| self.lower_operand(expr, depth))
            .collect()
    }

    fn slot_for(&mut self, data: &TableProcessorData) -> LoweredValue {
        let group = &mut *self.group;

        let slot = match data {
            TableProcessorData::ColumnInSourceRow(name) => *group
                .input_columns
                .entry(self.interner.get_or_intern(name.as_str()))
                .or_insert_with(|| Slot::Input(next_slot(&mut group.num_input_slots))),
            TableProcessorData::SchemaName => *group
                .input_schema_name
                .get_or_insert_with(|| Slot::Input(next_slot(&mut group.num_input_slots))),
            TableProcessorData::TableSuffix => {
                let prefix_length = self.processor.source_table.table.name.len();

                match group
                    .inputs_table_suffix
                    .binary_search_by_key(&prefix_length, |input| input.prefix_length)
                {
                    Ok(idx) => group.inputs_table_suffix[idx].slot,
                    Err(idx) => {
                        let slot = Slot::Input(next_slot(&mut group.num_input_slots));
                        group.inputs_table_suffix.insert(
                            idx,
                            TableSuffixInput {
                                prefix_length,
                                slot,
                            },
                        );
                        slot
                    }
                }
            }
            TableProcessorData::TableValuedFunction(output) => {
                let index = self
                    .processor
                    .table_valued_functions
                    .iter()
                    .position(|f| Arc::ptr_eq(f, &output.function))
                    .expect("table-valued function output must reference a function of the same processor");

                return LoweredValue {
                    slot: self.function_outputs[index][output.output_slot],
                    depth: index + 1,
                };
            }
        };

        LoweredValue { slot, depth: 0 }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use super::{
        EvaluatorInstruction, ProcessedGroupMember, ProcessedTableValuedFunction, ProcessorGroup,
    };
    use crate::sync_plan::{
        ReplicationEmbedder, StringInterner,
        expression::{SqlExpression, TableValuedFunction},
        plan::{
            StreamParameterIndexLookupCreator, TableProcessor, TableProcessorData,
            TableProcessorExpression, TableProcessorTableValuedFunction,
            TableProcessorTableValuedFunctionOutput,
        },
        table_pattern::TablePattern,
    };

    struct TestEmbedder;

    impl ReplicationEmbedder for TestEmbedder {
        type BucketId = ();
        type ParameterId = ();
        type EventId = ();
    }

    fn column(name: &str) -> TableProcessorExpression {
        SqlExpression::ExternalData(TableProcessorData::ColumnInSourceRow(Arc::new(
            name.to_string(),
        )))
    }

    fn processor() -> TableProcessor {
        let Ok(schema) = "public".parse();
        let Ok(table) = "users".parse();

        TableProcessor {
            source_table: TablePattern {
                connection_tag: "tag".to_string(),
                schema,
                table,
            },
            filters: vec![],
            parameters: vec![],
            table_valued_functions: vec![],
        }
    }

    #[test]
    fn evaluates_filters_before_expanding_functions() {
        // SELECT id FROM users, json_each(users.tags) WHERE users.active AND tags.value AND tags.key = $inputParameter
        let json_each = Arc::new(TableProcessorTableValuedFunction {
            function: TableValuedFunction::JsonEach,
            inputs: vec![column("tags")],
        });
        let output = |output_slot| {
            SqlExpression::ExternalData(TableProcessorData::TableValuedFunction(
                TableProcessorTableValuedFunctionOutput {
                    function: Arc::clone(&json_each),
                    output_slot,
                },
            ))
        };

        let source = StreamParameterIndexLookupCreator {
            processor: TableProcessor {
                filters: vec![column("active"), output(1)],
                parameters: vec![output(0)],
                table_valued_functions: vec![Arc::clone(&json_each)],
                ..processor()
            },
            outputs: vec![column("id")],
        };

        let mut interner = StringInterner::new();
        let mut group = ProcessorGroup::<TestEmbedder>::default();
        group.include_parameter(&mut interner, Arc::new(()), &source);

        let input = |name: &str| group.input_columns[&interner.get(name).unwrap()];
        let (tags, active, id) = (input("tags"), input("active"), input("id"));
        let ProcessedGroupMember::Parameter(source) = &group.processors[0] else {
            panic!("expected parameter source");
        };
        let instructions = &source.common.instructions;

        let [
            EvaluatorInstruction::CheckFilter(checked_active),
            EvaluatorInstruction::ExpandTableValuedFunction(ProcessedTableValuedFunction {
                function: TableValuedFunction::JsonEach,
                inputs,
                outputs: function_outputs,
            }),
            EvaluatorInstruction::CheckFilter(checked_value),
        ] = instructions.as_slice()
        else {
            panic!("unexpected instructions: {instructions:#?}");
        };

        // Should check the active column before evaluating the table-valued function.
        assert_eq!(*checked_active, active);
        assert_eq!(*inputs, [tags]);
        assert_eq!(function_outputs.len(), 2);
        assert_eq!(*checked_value, function_outputs[1]);
        assert_eq!(source.common.parameters, [function_outputs[0]]);
        assert_eq!(source.outputs, [id]);
        assert_eq!(group.num_input_slots, 3);
        assert_eq!(group.num_internal_slots, 2);
    }
}
