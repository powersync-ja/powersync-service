use std::{borrow::Cow, sync::Arc};

use serde_json::{Value, json, to_value};

use super::{Evaluator, RowEvaluation};
use crate::sync_plan::{
    ProcessedGroupMember, ProcessorGroup, SqliteValue,
    expression::{SqlExpression, TableValuedFunction},
    plan::{
        ColumnSource, RowProjection, StreamDataSource, StreamParameterIndexLookupCreator,
        TableProcessor, TableProcessorData, TableProcessorExpression,
        TableProcessorTableValuedFunction, TableProcessorTableValuedFunctionOutput,
    },
    test::{TestEmbedder, evaluator, source_table_ref, table_pattern},
    value::Parameter,
};

#[derive(Debug, PartialEq)]
struct RecordedRow {
    bucket: &'static str,
    parameters: String,
    table: String,
    columns: Value,
}

#[derive(Default)]
struct RecordingSink {
    rows: Vec<RecordedRow>,
    lookups: Vec<(&'static str, String, String)>,
}

fn format_parameters(values: &[Parameter<'_>]) -> String {
    serde_json::to_string(values).unwrap()
}

impl RecordingSink {
    fn record(&mut self, mut row: RowEvaluation<TestEmbedder>) {
        for member in row.members() {
            match member {
                ProcessedGroupMember::Data(source) => {
                    let mut buckets = row.process_data(source);
                    while let Some(data) = buckets.next() {
                        let data = data.unwrap();
                        self.rows.push(RecordedRow {
                            bucket: data.bucket,
                            parameters: format_parameters(data.parameters),
                            table: data.table.to_string(),
                            columns: to_value(&data.row).expect("serialize output row to json"),
                        });
                    }
                }
                ProcessedGroupMember::Parameter(source) => {
                    let mut lookups = row.process_parameters(source);
                    while let Some(lookup) = lookups.next() {
                        let lookup = lookup.unwrap();
                        self.lookups.push((
                            lookup.id,
                            format_parameters(lookup.lookup),
                            format_parameters(lookup.outputs),
                        ));
                    }
                }
            }
        }
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

/// A (name, value) pair representing a source row field.
type Field<'a> = (&'a str, SqliteValue<'a>);

/// Evaluates a row, returning the names of fields that were requested.
fn evaluate_row<'a>(
    evaluator: &mut Evaluator,
    group: &'a ProcessorGroup<TestEmbedder>,
    table_name: &'a str,
    fields: &[Field<'a>],
    sink: &mut RecordingSink,
) -> Vec<&'a str> {
    let mut requested = vec![];
    let mut row = evaluator.evaluate(source_table_ref(table_name), group);
    for (name, value) in fields {
        if let Some(field) = row.prepare_field(&Cow::Borrowed(name)) {
            requested.push(*name);
            field.set(*value);
        }
    }

    sink.record(row.finish());
    requested
}

#[test]
fn skips_unused_fields() {
    let mut source = data_source(
        "users",
        RowProjection::Columns(vec![alias("a", column("a"))]),
    );
    source.processor.filters.push(column("b"));
    let mut group = ProcessorGroup::default();
    group.include_data("b", &source);
    let mut sink = RecordingSink::default();

    let requested = evaluate_row(
        &mut evaluator(),
        &group,
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
        &mut evaluator(),
        &group,
        "users",
        &[("a", SqliteValue::Integer(2))],
        &mut sink,
    );

    assert_eq!(
        sink.rows,
        [RecordedRow {
            bucket: "b",
            parameters: "[]".to_string(),
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
    let mut group = ProcessorGroup::default();
    group.include_data("b", &source);
    let mut sink = RecordingSink::default();

    evaluate_row(
        &mut evaluator(),
        &group,
        "users",
        &[("a", SqliteValue::Integer(1))],
        &mut sink,
    );

    assert_eq!(
        sink.rows,
        [RecordedRow {
            bucket: "b",
            parameters: "[]".to_string(),
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
    let mut group = ProcessorGroup::default();
    group.include_data("b", &source);
    let mut sink = RecordingSink::default();

    evaluate_row(
        &mut evaluator(),
        &group,
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
        &mut evaluator(),
        &group,
        "users",
        &[("a", SqliteValue::Integer(1))],
        &mut sink,
    );
    assert_eq!(sink.rows[1].columns, json!({"x": 1, "y": 1, "a": 1}));
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
    let mut group = ProcessorGroup::default();
    group.include_data("b", &data);
    group.include_parameter("lookup", &lookup);

    let mut sink = RecordingSink::default();

    evaluate_row(
        &mut evaluator(),
        &group,
        "users",
        &[
            ("id", SqliteValue::Integer(1)),
            ("org", SqliteValue::Text("o")),
        ],
        &mut sink,
    );

    assert_eq!(sink.rows.len(), 1);
    assert_eq!(sink.rows[0].parameters, "[\"o\"]");
    assert_eq!(
        sink.lookups,
        [(
            "lookup",
            "[\"o\",\"public\",\"s\"]".to_string(),
            "[1]".to_string()
        )]
    );
}

fn json_each(input: &str) -> Arc<TableProcessorTableValuedFunction> {
    Arc::new(TableProcessorTableValuedFunction {
        function: TableValuedFunction::JsonEach,
        inputs: vec![column(input)],
    })
}

fn function_output(
    function: &Arc<TableProcessorTableValuedFunction>,
    output_slot: usize,
) -> TableProcessorExpression {
    SqlExpression::ExternalData(TableProcessorData::TableValuedFunction(
        TableProcessorTableValuedFunctionOutput {
            function: Arc::clone(function),
            output_slot,
        },
    ))
}

#[test]
fn nested_table_valued_functions() {
    // SELECT FROM users, json_each(users.a) a, json_each(users.b) b WHERE a.value
    //   AND a.key = $partition AND b.value = $partition
    let a = json_each("a");
    let b = json_each("b");
    let mut source = data_source("users", RowProjection::Columns(vec![]));
    source.processor.table_valued_functions = vec![a.clone(), b.clone()];
    source.processor.filters.push(function_output(&a, 1));
    source.processor.parameters = vec![function_output(&a, 0), function_output(&b, 1)];

    let mut group = ProcessorGroup::default();
    group.include_data("b", &source);
    let mut sink = RecordingSink::default();

    evaluate_row(
        &mut evaluator(),
        &group,
        "users",
        &[
            ("a", SqliteValue::Text(r#"{"a": 1, "b": 0, "c": 1}"#)),
            ("b", SqliteValue::Text(r#"["x", "y"]"#)),
        ],
        &mut sink,
    );
    let parameters: Vec<_> = sink.rows.iter().map(|r| r.parameters.as_str()).collect();
    assert_eq!(
        parameters,
        [
            r#"["a","x"]"#,
            r#"["a","y"]"#,
            r#"["c","x"]"#,
            r#"["c","y"]"#
        ]
    );

    // An empty inner function doesn't produce any rows.
    sink.rows.clear();
    evaluate_row(
        &mut evaluator(),
        &group,
        "users",
        &[
            ("a", SqliteValue::Text("[1, 1]")),
            ("b", SqliteValue::Text("[]")),
        ],
        &mut sink,
    );
    assert_eq!(sink.rows, []);
}

#[test]
fn continues_after_error() {
    let a = json_each("a");
    let mut failing = data_source("users", RowProjection::Columns(vec![]));
    failing.processor.table_valued_functions = vec![a.clone()];
    failing.processor.parameters = vec![function_output(&a, 1)];

    let mut group = ProcessorGroup::<TestEmbedder>::default();
    group.include_data("failing", &failing);
    group.include_data("b", &data_source("users", RowProjection::Columns(vec![])));

    let mut evaluator = evaluator();
    let mut row = evaluator.evaluate(source_table_ref("users"), &group);
    row.prepare_field("a")
        .unwrap()
        .set(SqliteValue::Text("invalid json"));
    let mut row = row.finish();

    let mut outputs = vec![];
    for member in row.members() {
        match member {
            ProcessedGroupMember::Data(source) => {
                let mut buckets = row.process_data(source);
                while let Some(result) = buckets.next() {
                    outputs.push(match result {
                        Ok(data) => format!("row {}", data.bucket),
                        Err(_) => format!("error {}", source.id),
                    });
                }
            }
            ProcessedGroupMember::Parameter(_) => unreachable!(),
        }
    }

    assert_eq!(outputs, ["error failing", "row b"]);
}
