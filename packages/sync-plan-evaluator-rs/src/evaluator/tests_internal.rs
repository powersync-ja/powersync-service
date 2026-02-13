#[test]
fn table_pattern_matches_wildcards() {
    let pattern = crate::model::SerializedTablePattern {
        connection: None,
        schema: None,
        table: "users%".to_string(),
    };

    let table = SourceTable {
        connection_tag: "default".to_string(),
        schema: "public".to_string(),
        name: "users_v2".to_string(),
    };

    assert!(table_matches(&pattern, &table));
}
