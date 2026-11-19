use std::{hint::black_box, io::Write, ops::Range, sync::Arc, time::Duration};

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use fake::{
    Fake,
    faker::{boolean::en::Boolean, chrono::en::DateTime, lorem::en::Sentence},
    rand::{SeedableRng, rngs::StdRng, seq::IndexedRandom},
    uuid::UUIDv4,
};
use powersync_service::sync_plan::{
    CompatibilityContext, DeserializeSyncPlan, EvaluationSink, Evaluator, HydratedSyncConfig,
    ProjectedRow, ReplicationEmbedder, SourceTableRef, SqliteValue,
};
use serde::Serialize;
use serde_json::Deserializer;

/// Approximate size of the generated source data, per format.
const DATA_SIZE: usize = 1 << 30;
/// A [ReplicationBatch] is flushed once its buffer grows beyond this size.
const BATCH_SIZE: usize = 16 << 20;
/// Amount of distinct users owning lists.
const USERS: usize = 10_000;

/// Evaluates two simple Sync Streams on an in-memory data set.
///
/// This uses the following Sync Config as a fixture:
///
/// ```yaml
/// config:
///   edition: 3
/// streams:
///   my_lists:
///     auto_subscribe: true
///     query: SELECT * FROM lists WHERE owner = auth.user_id()
///   contents:
///     query: | SELECT todo_items.* FROM todo_items
///         INNER JOIN lists ON todo_items.list = lists.id
///         WHERE lists.owner = auth.user_id()
///         AND todo_items.list = subscription.parameter('list')
/// ```
struct BenchmarkEmbedder;

impl ReplicationEmbedder for BenchmarkEmbedder {
    type BucketId = &'static str;
    type ParameterId = &'static str;
    type EventId = &'static str;
}

const CONNECTION_TAG: &str = "tag";
const SCHEMA: &str = "schema";

fn sync_config() -> HydratedSyncConfig<BenchmarkEmbedder> {
    let my_lists = r#"
{
  "hash": 385587389,
  "table": {
    "connection": null,
    "schema": null,
    "table": "lists"
  },
  "outputTableName": "lists",
  "tableValuedFunctions": [],
  "filters": [],
  "partitionBy": [
    {
      "expr": {
        "type": "data",
        "source": {
        "column": "owner"
        }
      }
    }
  ],
  "columns": [
    "star"
  ]
}"#;
    let contents_data = r#"
{
  "hash": 284009032,
  "table": {
    "connection": null,
    "schema": null,
    "table": "todo_items"
  },
  "outputTableName": "todo_items",
  "tableValuedFunctions": [],
  "filters": [],
  "partitionBy": [
    {
      "expr": {
        "type": "data",
        "source": {
          "column": "list"
        }
      }
    }
  ],
  "columns": [
    "star"
  ]
}
"#;
    let contents_parameter = r#"
{
  "hash": 323765117,
  "table": {
    "connection": null,
    "schema": null,
    "table": "lists"
  },
  "tableValuedFunctions": [],
  "filters": [],
  "partitionBy": [
    {
      "expr": {
        "type": "data",
        "source": {
            "column": "owner"
        }
      }
    }
  ],
  "output": [
    {
        "type": "data",
        "source": {
        "column": "id"
        }
    }
  ],
  "lookupScope": {
    "lookupName": "lookup",
    "queryId": "0"
  }
}
"#;

    let compat = CompatibilityContext::default();
    let deserialize = DeserializeSyncPlan {
        default_connection_tag: CONNECTION_TAG,
        default_schema: SCHEMA,
    };

    HydratedSyncConfig::new(
        compat,
        &[
            deserialize
                .data_source(Arc::new("my_lists"), &mut Deserializer::from_str(my_lists))
                .expect("decode my_lists"),
            deserialize
                .data_source(
                    Arc::new("contents"),
                    &mut Deserializer::from_str(contents_data),
                )
                .expect("decode contents"),
            deserialize
                .parameter_source(
                    Arc::new("contents"),
                    &mut Deserializer::from_str(contents_parameter),
                )
                .expect("decode contents parameter"),
        ],
    )
}

struct ReplicationBatch {
    sink: Vec<u8>,
}

impl ReplicationBatch {
    fn new() -> Self {
        Self {
            // Leave some headroom so that the last row before a flush doesn't cause a reallocation.
            sink: Vec::with_capacity(BATCH_SIZE + (1 << 20)),
        }
    }

    fn flush(&mut self) {
        black_box(&self.sink);
        self.sink.clear();
    }
}

impl EvaluationSink<BenchmarkEmbedder> for ReplicationBatch {
    // Kind of simulate what a replicator would do: Serialize bucket parameters, rows and parameters
    // into an opaque buffer.

    fn row(
        &mut self,
        bucket: &&str,
        parameters: &[SqliteValue<'_>],
        table: &str,
        row: &ProjectedRow<'_, '_>,
    ) {
        let _ = write!(&mut self.sink, "{bucket}#");
        serde_json::to_writer(&mut self.sink, parameters).unwrap();

        let _ = write!(&mut self.sink, "{table}: ");
        serde_json::to_writer(&mut self.sink, row).unwrap();
        self.sink.push(b'\n');
    }

    fn parameter_lookup(
        &mut self,
        id: &&str,
        lookup: &[SqliteValue<'_>],
        outputs: &[SqliteValue<'_>],
    ) {
        let _ = write!(&mut self.sink, "{id}:");
        serde_json::to_writer(&mut self.sink, lookup).unwrap();
        let _ = write!(&mut self.sink, " -> ");
        serde_json::to_writer(&mut self.sink, outputs).unwrap();
        self.sink.push(b'\n');
    }

    fn event(&mut self, _id: &&str) {
        panic!("Did not expect event")
    }

    fn error(
        &mut self,
        _source: powersync_service::sync_plan::ErrorSource<BenchmarkEmbedder>,
        error: powersync_service::sync_plan::EvaluationError,
    ) {
        panic!("Evaluation error: {error}");
    }
}

#[derive(Clone, Copy)]
enum Format {
    Json,
    Bson,
}

impl Format {
    fn name(self) -> &'static str {
        match self {
            Format::Json => "json",
            Format::Bson => "bson",
        }
    }
}

#[derive(Clone, Copy)]
enum Table {
    Lists,
    TodoItems,
}

impl Table {
    fn source_ref(self) -> SourceTableRef<'static> {
        SourceTableRef {
            connection_tag: CONNECTION_TAG,
            schema: SCHEMA,
            name: match self {
                Table::Lists => "lists",
                Table::TodoItems => "todo_items",
            },
        }
    }
}

trait SourceRow: Serialize {
    const TABLE: Table;
}

#[derive(Serialize)]
struct ListRow {
    id: String,
    owner: String,
    name: String,
    description: String,
    created_at: String,
    archived: bool,
    position: i64,
}

impl ListRow {
    fn generate(rng: &mut StdRng, users: &[String]) -> Self {
        Self {
            id: UUIDv4.fake_with_rng(rng),
            owner: users.choose(rng).unwrap().clone(),
            name: Sentence(1..5).fake_with_rng(rng),
            description: Sentence(5..40).fake_with_rng(rng),
            created_at: DateTime().fake_with_rng(rng),
            archived: Boolean(10).fake_with_rng(rng),
            position: (0..1000).fake_with_rng(rng),
        }
    }
}

impl SourceRow for ListRow {
    const TABLE: Table = Table::Lists;
}

#[derive(Serialize)]
struct TodoItemRow {
    id: String,
    list: String,
    created_by: String,
    description: String,
    created_at: String,
    completed: bool,
    completed_at: Option<String>,
    priority: i64,
    estimate_hours: f64,
}

impl TodoItemRow {
    fn generate(rng: &mut StdRng, list: &ListRow, users: &[String]) -> Self {
        let completed = Boolean(40).fake_with_rng(rng);

        Self {
            id: UUIDv4.fake_with_rng(rng),
            list: list.id.clone(),
            created_by: users.choose(rng).unwrap().clone(),
            description: Sentence(3..30).fake_with_rng(rng),
            created_at: DateTime().fake_with_rng(rng),
            completed,
            completed_at: completed.then(|| DateTime().fake_with_rng(rng)),
            priority: (0..5).fake_with_rng(rng),
            estimate_hours: (0.25..40.0).fake_with_rng(rng),
        }
    }
}

impl SourceRow for TodoItemRow {
    const TABLE: Table = Table::TodoItems;
}

/// Encoded source rows, stored in a single buffer.
struct Dataset {
    format: Format,
    data: Vec<u8>,
    rows: Vec<(Table, Range<usize>)>,
}

impl Dataset {
    /// Generates around [DATA_SIZE] bytes of lists with an average of 10 todo items each.
    ///
    /// The RNG is seeded, so each format encodes the same rows.
    fn generate(format: Format) -> Self {
        let mut rng = StdRng::seed_from_u64(0);
        let users: Vec<String> = (0..USERS).map(|_| UUIDv4.fake_with_rng(&mut rng)).collect();
        let mut dataset = Self {
            format,
            data: Vec::with_capacity(DATA_SIZE + (1 << 20)),
            rows: vec![],
        };

        while dataset.data.len() < DATA_SIZE {
            let list = ListRow::generate(&mut rng, &users);
            dataset.push(&list);

            for _ in 0..(0..=20).fake_with_rng::<usize, _>(&mut rng) {
                dataset.push(&TodoItemRow::generate(&mut rng, &list, &users));
            }
        }

        dataset
    }

    fn push<R: SourceRow>(&mut self, row: &R) {
        let start = self.data.len();
        match self.format {
            Format::Json => serde_json::to_writer(&mut self.data, row).unwrap(),
            Format::Bson => bson::serialize_to_buffer(row, &mut self.data).unwrap(),
        }
        self.rows.push((R::TABLE, start..self.data.len()));
    }

    /// Evaluates all rows, writing outputs into the batch and flushing it when it gets too large.
    fn evaluate(&self, evaluator: &mut Evaluator<BenchmarkEmbedder>, batch: &mut ReplicationBatch) {
        for (table, range) in &self.rows {
            let bytes = &self.data[range.clone()];
            let Some(mut row) = evaluator.evaluate(table.source_ref()) else {
                continue;
            };

            match self.format {
                Format::Json => row
                    .push_json(&mut serde_json::Deserializer::from_slice(bytes))
                    .unwrap(),
                Format::Bson => row.push_bson(bytes).unwrap(),
            }
            row.finish(batch);

            if batch.sink.len() >= BATCH_SIZE {
                batch.flush();
            }
        }

        batch.flush();
    }
}

fn evaluate_sync_plan(c: &mut Criterion) {
    let config = Arc::new(sync_config());
    let mut group = c.benchmark_group("evaluate_sync_plan");
    // Each iteration processes the full data set, so we can't afford many samples.
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(30));

    for format in [Format::Json, Format::Bson] {
        // Generated inside the loop so that only one data set is kept in memory at a time.
        let dataset = Dataset::generate(format);
        group.throughput(Throughput::Bytes(dataset.data.len() as u64));

        group.bench_function(format.name(), |b| {
            let mut evaluator = Evaluator::new(config.clone());
            let mut batch = ReplicationBatch::new();
            b.iter(|| dataset.evaluate(&mut evaluator, &mut batch));
        });
    }

    group.finish();
}

criterion_group!(benches, evaluate_sync_plan);
criterion_main!(benches);
