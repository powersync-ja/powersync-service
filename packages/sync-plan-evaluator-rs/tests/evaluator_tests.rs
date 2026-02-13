use serde_json::{json, Map, Value};
use std::process::Command;
use sync_plan_evaluator_rs::{model::*, EvaluatorOptions, SyncPlanEvaluator};

fn source_table(name: &str) -> SourceTable {
    SourceTable {
        connection_tag: "default".to_string(),
        schema: "test_schema".to_string(),
        name: name.to_string(),
    }
}

#[test]
fn evaluates_data_rows_for_simple_partition() {
    let plan_json = json!({
      "version": "unstable",
      "dataSources": [
        {
          "hash": 1,
          "table": { "connection": null, "schema": null, "table": "users" },
          "outputTableName": "users",
          "filters": [],
          "partitionBy": [
            {
              "expr": { "type": "data", "source": { "column": "value" } }
            }
          ],
          "columns": ["star"]
        }
      ],
      "buckets": [
        { "hash": 1, "uniqueName": "stream|0", "sources": [0] }
      ],
      "parameterIndexes": [],
      "streams": []
    })
    .to_string();

    let evaluator =
        SyncPlanEvaluator::from_serialized_json(&plan_json, EvaluatorOptions::default()).unwrap();
    let mut row = Map::new();
    row.insert("id".to_string(), json!("foo"));
    row.insert("value".to_string(), json!(1));

    let evaluated = evaluator
        .evaluate_row(EvaluateRowOptions {
            source_table: source_table("users"),
            record: row,
        })
        .unwrap();

    assert_eq!(evaluated.len(), 1);
    assert_eq!(evaluated[0].bucket, "stream|0[1]");
    assert_eq!(evaluated[0].id, "foo");
}

#[test]
fn star_projection_keeps_underscore_columns_when_json_scalar() {
    let plan_json = json!({
      "version": "unstable",
      "dataSources": [
        {
          "hash": 1,
          "table": { "connection": null, "schema": null, "table": "users" },
          "outputTableName": "users",
          "filters": [],
          "partitionBy": [],
          "columns": ["star"]
        }
      ],
      "buckets": [
        { "hash": 1, "uniqueName": "stream|0", "sources": [0] }
      ],
      "parameterIndexes": [],
      "streams": []
    })
    .to_string();

    let evaluator =
        SyncPlanEvaluator::from_serialized_json(&plan_json, EvaluatorOptions::default()).unwrap();
    let evaluated = evaluator
        .evaluate_row(EvaluateRowOptions {
            source_table: source_table("users"),
            record: json!({
                "id": "foo",
                "_double": 1.5,
                "_int": 1,
                "_null": null,
                "_text": "text",
                "_blob_like": {"k": "v"}
            })
            .as_object()
            .unwrap()
            .clone(),
        })
        .unwrap();

    assert_eq!(evaluated.len(), 1);
    assert_eq!(
        evaluated[0].data,
        json!({
            "id": "foo",
            "_double": 1.5,
            "_int": 1,
            "_null": null,
            "_text": "text"
        })
        .as_object()
        .unwrap()
        .clone()
    );
}

#[test]
fn evaluates_parameter_index_rows() {
    let plan_json = json!({
      "version": "unstable",
      "dataSources": [],
      "buckets": [],
      "parameterIndexes": [
        {
          "hash": 1,
          "table": { "connection": null, "schema": null, "table": "issues" },
          "filters": [],
          "partitionBy": [
            { "expr": { "type": "data", "source": { "column": "owner_id" } } }
          ],
          "output": [
            { "type": "data", "source": { "column": "id" } }
          ],
          "lookupScope": { "lookupName": "lookup", "queryId": "0" }
        }
      ],
      "streams": []
    })
    .to_string();

    let evaluator =
        SyncPlanEvaluator::from_serialized_json(&plan_json, EvaluatorOptions::default()).unwrap();

    let mut row = Map::new();
    row.insert("id".to_string(), json!("issue_id"));
    row.insert("owner_id".to_string(), json!("user1"));

    let parameter_rows = evaluator
        .evaluate_parameter_row(&source_table("issues"), &row)
        .unwrap();
    assert_eq!(parameter_rows.len(), 1);
    assert_eq!(
        parameter_rows[0].lookup.serialized_representation,
        "[\"lookup\",\"0\",\"user1\"]"
    );
    assert_eq!(
        parameter_rows[0].bucket_parameters,
        vec![json!({"0": "issue_id"}).as_object().unwrap().clone()]
    );
}

#[test]
fn parity_with_js_for_evaluate_row() {
    let node_payload = run_node(
        r#"
import { SqlSyncRules, serializeSyncPlan } from './packages/sync-rules/dist/index.js';
const yaml = `
config:
  edition: 2
  sync_config_compiler: true
streams:
  stream:
    query: SELECT * FROM users WHERE value = subscription.parameter('p')
`;
const {config} = SqlSyncRules.fromYaml(yaml,{defaultSchema:'test_schema',throwOnError:true,allowNewSyncCompiler:true});
const hydrated = config.hydrate();
const rows = hydrated.evaluateRow({sourceTable:{connectionTag:'default',schema:'test_schema',name:'users'},record:{id:'foo',value:1}});
console.log(JSON.stringify({ plan: serializeSyncPlan(config.plan), rows }));
"#,
    );

    let plan_json = serde_json::to_string(&node_payload["plan"]).unwrap();
    let expected_rows = node_payload["rows"].clone();

    let evaluator =
        SyncPlanEvaluator::from_serialized_json(&plan_json, EvaluatorOptions::default()).unwrap();
    let mut row = Map::new();
    row.insert("id".to_string(), json!("foo"));
    row.insert("value".to_string(), json!(1));

    let rows = evaluator
        .evaluate_row(EvaluateRowOptions {
            source_table: source_table("users"),
            record: row,
        })
        .unwrap();

    assert_eq!(serde_json::to_value(rows).unwrap(), expected_rows);
}

#[test]
fn parity_with_js_for_wildcard_alias_evaluate_row() {
    let node_payload = run_node(
        r#"
import { SqlSyncRules, serializeSyncPlan } from './packages/sync-rules/dist/index.js';
const yaml = `
config:
  edition: 2
  sync_config_compiler: true
streams:
  stream:
    query: SELECT * FROM "%" output
`;
const {config} = SqlSyncRules.fromYaml(yaml,{defaultSchema:'test_schema',throwOnError:true,allowNewSyncCompiler:true});
const hydrated = config.hydrate();
const rows = hydrated.evaluateRow({sourceTable:{connectionTag:'default',schema:'test_schema',name:'users'},record:{id:'foo'}});
console.log(JSON.stringify({ plan: serializeSyncPlan(config.plan), rows }));
"#,
    );

    let plan_json = serde_json::to_string(&node_payload["plan"]).unwrap();
    let expected_rows = node_payload["rows"].clone();

    let evaluator =
        SyncPlanEvaluator::from_serialized_json(&plan_json, EvaluatorOptions::default()).unwrap();

    let rows = evaluator
        .evaluate_row(EvaluateRowOptions {
            source_table: source_table("users"),
            record: json!({"id": "foo"}).as_object().unwrap().clone(),
        })
        .unwrap();

    assert_eq!(serde_json::to_value(rows).unwrap(), expected_rows);
}

#[test]
fn parity_with_js_for_multiple_queries_same_bucket() {
    let node_payload = run_node(
        r#"
import { SqlSyncRules, serializeSyncPlan } from './packages/sync-rules/dist/index.js';
const yaml = `
config:
  edition: 2
  sync_config_compiler: true
streams:
  stream:
    queries:
      - SELECT * FROM users
      - SELECT * FROM comments
`;
const {config} = SqlSyncRules.fromYaml(yaml,{defaultSchema:'test_schema',throwOnError:true,allowNewSyncCompiler:true});
const hydrated = config.hydrate();
const rowsUsers = hydrated.evaluateRow({sourceTable:{connectionTag:'default',schema:'test_schema',name:'users'},record:{id:'u1'}});
const rowsComments = hydrated.evaluateRow({sourceTable:{connectionTag:'default',schema:'test_schema',name:'comments'},record:{id:'c1'}});
console.log(JSON.stringify({ plan: serializeSyncPlan(config.plan), rowsUsers, rowsComments }));
"#,
    );

    let evaluator = SyncPlanEvaluator::from_serialized_json(
        &serde_json::to_string(&node_payload["plan"]).unwrap(),
        EvaluatorOptions::default(),
    )
    .unwrap();

    let rows_users = evaluator
        .evaluate_row(EvaluateRowOptions {
            source_table: source_table("users"),
            record: json!({"id": "u1"}).as_object().unwrap().clone(),
        })
        .unwrap();
    let rows_comments = evaluator
        .evaluate_row(EvaluateRowOptions {
            source_table: source_table("comments"),
            record: json!({"id": "c1"}).as_object().unwrap().clone(),
        })
        .unwrap();

    assert_eq!(
        serde_json::to_value(rows_users).unwrap(),
        node_payload["rowsUsers"]
    );
    assert_eq!(
        serde_json::to_value(rows_comments).unwrap(),
        node_payload["rowsComments"]
    );
}

#[test]
fn parity_with_js_for_prepare_bucket_queries_request_data() {
    let node_payload = run_node(
        r#"
import { BaseJwtPayload, RequestParameters, SqlSyncRules, serializeSyncPlan } from './packages/sync-rules/dist/index.js';
const yaml = `
config:
  edition: 2
  sync_config_compiler: true
streams:
  stream:
    auto_subscribe: true
    query: SELECT * FROM issues WHERE owner = auth.user_id()
`;
const {config} = SqlSyncRules.fromYaml(yaml,{defaultSchema:'test_schema',throwOnError:true,allowNewSyncCompiler:true});
const hydrated = config.hydrate();
const {querier, errors} = hydrated.getBucketParameterQuerier({
  globalParameters: new RequestParameters(new BaseJwtPayload({sub:'user'}), {}),
  hasDefaultStreams: true,
  streams: {}
});
console.log(JSON.stringify({
  plan: serializeSyncPlan(config.plan),
  errors,
  staticBuckets: querier.staticBuckets,
  dynamicQueries: querier.dynamicQueries ?? []
}));
"#,
    );

    let evaluator = SyncPlanEvaluator::from_serialized_json(
        &serde_json::to_string(&node_payload["plan"]).unwrap(),
        EvaluatorOptions::default(),
    )
    .unwrap();

    let prepared = evaluator
        .prepare_bucket_queries(PrepareBucketQueryOptions {
            global_parameters: RequestParameters {
                auth: json!({"sub": "user"}),
                connection: json!({}),
                subscription: json!({}),
            },
            has_default_streams: true,
            streams: std::collections::BTreeMap::new(),
        })
        .unwrap();

    assert_eq!(node_payload["errors"], json!([]));
    assert_eq!(
        serde_json::to_value(&prepared.static_buckets).unwrap(),
        node_payload["staticBuckets"]
    );
    assert_eq!(
        serde_json::to_value(&prepared.dynamic_queries).unwrap(),
        node_payload["dynamicQueries"]
    );
}

#[test]
fn resolves_dynamic_bucket_queries() {
    let node_payload = run_node(
        r#"
import { SqlSyncRules, serializeSyncPlan } from './packages/sync-rules/dist/index.js';
const yaml = `
config:
  edition: 2
  sync_config_compiler: true
streams:
  stream:
    query: SELECT * FROM comments WHERE issue_id IN (SELECT id FROM issues WHERE owner_id = auth.user_id())
`;
const {config} = SqlSyncRules.fromYaml(yaml,{defaultSchema:'test_schema',throwOnError:true,allowNewSyncCompiler:true});
console.log(JSON.stringify({ plan: serializeSyncPlan(config.plan) }));
"#,
    );

    let evaluator = SyncPlanEvaluator::from_serialized_json(
        &serde_json::to_string(&node_payload["plan"]).unwrap(),
        EvaluatorOptions::default(),
    )
    .unwrap();

    let prepared = evaluator
        .prepare_bucket_queries(PrepareBucketQueryOptions {
            global_parameters: RequestParameters {
                auth: json!({"sub": "user1"}),
                connection: json!({}),
                subscription: json!({}),
            },
            has_default_streams: true,
            streams: std::collections::BTreeMap::from([(
                "stream".to_string(),
                vec![RequestedStream {
                    opaque_id: 7,
                    parameters: None,
                }],
            )]),
        })
        .unwrap();

    assert!(prepared.static_buckets.is_empty());
    assert_eq!(prepared.dynamic_queries.len(), 1);

    let lookup = &prepared.dynamic_queries[0].lookup_requests[0].values[0];
    assert_eq!(
        lookup.serialized_representation,
        "[\"lookup\",\"0\",\"user1\"]"
    );

    let buckets = evaluator
        .resolve_bucket_queries(
            &prepared,
            &[LookupResults {
                lookup: lookup.clone(),
                rows: vec![json!({"0": "issue_id"}).as_object().unwrap().clone()],
            }],
        )
        .unwrap();

    assert_eq!(buckets.len(), 1);
    assert_eq!(buckets[0].bucket, "stream|0[\"issue_id\"]");
}

#[test]
fn resolves_multi_stage_dynamic_lookup_queries() {
    let node_payload = run_node(
        r#"
import { SqlSyncRules, serializeSyncPlan } from './packages/sync-rules/dist/index.js';
const yaml = `
config:
  edition: 2
  sync_config_compiler: true
streams:
  stream:
    auto_subscribe: true
    query: |
      SELECT c.* FROM comments c
        INNER JOIN issues i ON c.issue = i.id
        INNER JOIN users owner ON owner.name = i.owned_by
      WHERE owner.id = auth.user_id()
`;
const {config} = SqlSyncRules.fromYaml(yaml,{defaultSchema:'test_schema',throwOnError:true,allowNewSyncCompiler:true});
console.log(JSON.stringify({ plan: serializeSyncPlan(config.plan) }));
"#,
    );

    let evaluator = SyncPlanEvaluator::from_serialized_json(
        &serde_json::to_string(&node_payload["plan"]).unwrap(),
        EvaluatorOptions::default(),
    )
    .unwrap();

    let prepared = evaluator
        .prepare_bucket_queries(PrepareBucketQueryOptions {
            global_parameters: RequestParameters {
                auth: json!({"sub": "user"}),
                connection: json!({}),
                subscription: json!({}),
            },
            has_default_streams: true,
            streams: std::collections::BTreeMap::new(),
        })
        .unwrap();

    assert!(prepared.static_buckets.is_empty());
    assert_eq!(prepared.dynamic_queries.len(), 1);
    assert_eq!(prepared.dynamic_queries[0].lookup_requests.len(), 1);

    let first_lookup = prepared.dynamic_queries[0].lookup_requests[0].values[0].clone();
    let second_lookup = ScopedParameterLookup {
        serialized_representation: r#"["lookup","1","name"]"#.to_string(),
        values: vec![json!("lookup"), json!("1"), json!("name")],
    };

    let buckets = evaluator
        .resolve_bucket_queries(
            &prepared,
            &[
                LookupResults {
                    lookup: first_lookup,
                    rows: vec![json!({"0": "name"}).as_object().unwrap().clone()],
                },
                LookupResults {
                    lookup: second_lookup,
                    rows: vec![json!({"0": "issue"}).as_object().unwrap().clone()],
                },
            ],
        )
        .unwrap();

    assert_eq!(buckets.len(), 1);
    assert_eq!(buckets[0].bucket, "stream|0[\"issue\"]");
}

fn run_node(source: &str) -> Value {
    let output = Command::new("node")
        .current_dir("../..")
        .arg("--input-type=module")
        .arg("-e")
        .arg(source)
        .output()
        .expect("node should execute");

    if !output.status.success() {
        panic!(
            "node script failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    serde_json::from_slice::<Value>(&output.stdout).expect("node output must be JSON")
}
