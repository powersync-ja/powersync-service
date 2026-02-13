#[test]
fn compiles_basic_query() {
    let yaml = r#"
config:
  edition: 2
  sync_config_compiler: true
streams:
  stream:
    query: SELECT * FROM users
"#;

    let compiled = compile_sync_plan(yaml, CompileOptions::default()).unwrap();
    assert_eq!(compiled.plan.streams.len(), 1);
    assert_eq!(compiled.plan.data_sources.len(), 1);
    assert_eq!(compiled.plan.buckets[0].unique_name, "stream|0");
    assert!(compiled.plan.parameter_indexes.is_empty());
}

#[test]
fn extracts_request_partition_from_where_clause() {
    let yaml = r#"
config:
  edition: 2
  sync_config_compiler: true
streams:
  stream:
    query: SELECT * FROM users WHERE value = subscription.parameter('p')
"#;

    let compiled = compile_sync_plan(yaml, CompileOptions::default()).unwrap();
    let source = &compiled.plan.data_sources[0];
    assert_eq!(source.partition_by.len(), 1);
    assert!(source.filters.is_empty());

    let querier = &compiled.plan.streams[0].queriers[0];
    assert_eq!(querier.source_instantiation.len(), 1);
    assert!(querier.request_filters.is_empty());
}

#[test]
fn reuses_bucket_for_multiple_queries_in_stream() {
    let yaml = r#"
config:
  edition: 2
  sync_config_compiler: true
streams:
  stream:
    queries:
      - SELECT * FROM users
      - SELECT * FROM comments
"#;

    let compiled = compile_sync_plan(yaml, CompileOptions::default()).unwrap();
    assert_eq!(compiled.plan.buckets.len(), 1);
    assert_eq!(compiled.plan.buckets[0].sources.len(), 2);
    assert_eq!(compiled.plan.streams[0].queriers.len(), 1);
    assert_eq!(compiled.plan.streams[0].queriers[0].bucket, 0);
}
