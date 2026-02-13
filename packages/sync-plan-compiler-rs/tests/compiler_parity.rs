use serde_json::Value;
use std::process::Command;
use sync_plan_compiler_rs::{compile_sync_plan, CompileOptions};

#[test]
fn parity_with_js_for_basic_query() {
    let yaml = r#"
config:
  edition: 2
  sync_config_compiler: true
streams:
  stream:
    query: SELECT * FROM users
"#;

    assert_plan_parity(yaml);
}

#[test]
fn parity_with_js_for_request_partition_query() {
    let yaml = r#"
config:
  edition: 2
  sync_config_compiler: true
streams:
  stream:
    query: SELECT * FROM users WHERE value = subscription.parameter('p')
"#;

    assert_plan_parity(yaml);
}

#[test]
fn parity_with_js_for_auth_user_id_and_row_filter() {
    let yaml = r#"
config:
  edition: 2
  sync_config_compiler: true
streams:
  stream:
    query: SELECT * FROM users WHERE id = auth.user_id() AND active
"#;

    assert_plan_parity(yaml);
}

#[test]
fn parity_with_js_for_wildcard_table_alias() {
    let yaml = r#"
config:
  edition: 2
  sync_config_compiler: true
streams:
  stream:
    query: SELECT * FROM "%" output
"#;

    assert_plan_parity(yaml);
}

#[test]
fn parity_with_js_for_multiple_queries_in_one_stream() {
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

    assert_plan_parity(yaml);
}

#[test]
fn parity_with_js_for_between_cast_and_null_checks() {
    let yaml = r#"
config:
  edition: 2
  sync_config_compiler: true
streams:
  stream:
    query: SELECT id, CAST(value AS TEXT) AS value_text FROM users WHERE count BETWEEN 1 AND 3 AND deleted_at IS NULL
"#;

    assert_plan_parity(yaml);
}

#[test]
fn parity_with_js_for_subquery_lookup() {
    let yaml = r#"
config:
  edition: 2
  sync_config_compiler: true
streams:
  stream:
    query: SELECT * FROM comments WHERE issue_id IN (SELECT id FROM issues WHERE owner_id = auth.user_id())
"#;

    assert_plan_parity(yaml);
}

#[test]
fn parity_with_js_for_join_chain_lookup_stages() {
    let yaml = r#"
config:
  edition: 2
  sync_config_compiler: true
streams:
  stream:
    query: |
      SELECT c.* FROM comments c
        INNER JOIN issues i ON c.issue = i.id
        INNER JOIN users owner ON owner.name = i.owned_by
      WHERE owner.id = auth.user_id()
"#;

    assert_plan_parity(yaml);
}

#[test]
fn parity_with_js_for_order_independent_parameter_reuse() {
    let yaml = r#"
config:
  edition: 2
  sync_config_compiler: true
streams:
  stream:
    queries:
      - SELECT * FROM stores WHERE region = subscription.parameter('region') AND org = auth.parameter('org')
      - SELECT * FROM products WHERE org = auth.parameter('org') AND region = subscription.parameter('region')
"#;

    assert_plan_parity(yaml);
}

#[test]
fn parity_with_js_for_bucket_reuse_between_streams() {
    let yaml = r#"
config:
  edition: 2
  sync_config_compiler: true
streams:
  a:
    query: SELECT * FROM profiles WHERE "user" = auth.user_id()
  b:
    query: SELECT * FROM profiles WHERE "user" IN (SELECT member FROM orgs WHERE id = auth.parameter('org'))
"#;

    assert_plan_parity(yaml);
}

#[test]
fn parity_with_js_for_parameter_index_reuse_between_streams() {
    let yaml = r#"
config:
  edition: 2
  sync_config_compiler: true
streams:
  a:
    query: SELECT * FROM projects WHERE org IN (SELECT org FROM users WHERE id = auth.user_id())
  b:
    query: SELECT * FROM subscriptions WHERE org IN (SELECT org FROM users WHERE id = auth.user_id())
"#;

    assert_plan_parity(yaml);
}

#[test]
fn parity_with_js_for_join_response_1() {
    let yaml = r#"
config:
  edition: 2
  sync_config_compiler: true
streams:
  stream:
    query: |
      SELECT u.*
        FROM users u
        JOIN group_memberships gm1 ON u.id = gm1.user_id
        JOIN group_memberships gm2 ON gm1.group_id = gm2.group_id
      WHERE gm2.user_id = auth.user_id()
"#;

    assert_plan_parity(yaml);
}

#[test]
fn parity_with_js_for_join_response_4() {
    let yaml = r#"
config:
  edition: 2
  sync_config_compiler: true
streams:
  stream:
    query: |
      SELECT m.*
        FROM message m
        JOIN roles srm
          ON m.organization_id = srm.organization_id
      WHERE srm.account_id = auth.user_id()
        AND srm.role_id = 'ORGANIZATION_LEADER'
"#;

    assert_plan_parity(yaml);
}

#[test]
fn parity_with_js_for_join_response_11() {
    let yaml = r#"
config:
  edition: 2
  sync_config_compiler: true
streams:
  stream:
    query: |
      SELECT t.*
      FROM ticket t
      JOIN ticket_detail_item item ON item.ticket_id = t.id
      WHERE item.user_id = auth.user_id()
"#;

    assert_plan_parity(yaml);
}

#[test]
fn parity_with_js_for_subquery_response_8() {
    let yaml = r#"
config:
  edition: 2
  sync_config_compiler: true
streams:
  stream:
    query: SELECT * FROM events WHERE id IN (SELECT event_id FROM event_users WHERE user_id = auth.user_id())
"#;

    assert_plan_parity(yaml);
}

#[test]
fn parity_with_js_for_nested_subqueries_response_5() {
    let yaml = r#"
config:
  edition: 2
  sync_config_compiler: true
streams:
  stream:
    queries:
      - SELECT * FROM users WHERE family_id IN (SELECT users.family_id FROM users INNER JOIN user_auth_mappings ON users.id = user_auth_mappings.user_id WHERE user_auth_mappings.auth_id = auth.user_id())
      - SELECT * FROM families WHERE id IN (SELECT users.family_id FROM users INNER JOIN user_auth_mappings ON users.id = user_auth_mappings.user_id WHERE user_auth_mappings.auth_id = auth.user_id())
"#;

    assert_plan_parity(yaml);
}

#[test]
fn parity_with_js_for_from_subquery_inner_names() {
    let yaml = r#"
config:
  edition: 2
  sync_config_compiler: true
streams:
  stream:
    query: SELECT * FROM (SELECT id, name FROM users) AS subquery
"#;

    assert_plan_parity(yaml);
}

#[test]
fn parity_with_js_for_from_subquery_outer_names() {
    let yaml = r#"
config:
  edition: 2
  sync_config_compiler: true
streams:
  stream:
    query: SELECT * FROM (SELECT id, name FROM users) AS subquery (my_id, custom_name)
"#;

    assert_plan_parity(yaml);
}

#[test]
fn parity_with_js_for_cte_as_data_source() {
    let yaml = r#"
config:
  edition: 2
  sync_config_compiler: true
streams:
  stream:
    auto_subscribe: true
    with:
      org_of_user: |
        SELECT id, name FROM organisations
          WHERE id IN (SELECT org_id FROM org_memberships WHERE user_id = auth.user_id())
    query: SELECT * FROM org_of_user
"#;

    assert_plan_parity(yaml);
}

#[test]
fn parity_with_js_for_cte_as_parameter_query() {
    let yaml = r#"
config:
  edition: 2
  sync_config_compiler: true
streams:
  stream:
    auto_subscribe: true
    with:
      org_of_user: |
        SELECT id, name FROM organisations
          WHERE id IN (SELECT org_id FROM org_memberships WHERE user_id = auth.user_id())
    query: SELECT * FROM projects WHERE org_id IN (SELECT id FROM org_of_user)
"#;

    assert_plan_parity(yaml);
}

#[test]
fn parity_with_js_for_in_array() {
    let yaml = r#"
config:
  edition: 2
  sync_config_compiler: true
streams:
  stream:
    query: SELECT * FROM notes WHERE state IN ARRAY['public', 'archived']
"#;

    assert_plan_parity(yaml);
}

#[test]
fn parity_with_js_for_row_and_request_filter_combination() {
    let yaml = r#"
config:
  edition: 2
  sync_config_compiler: true
streams:
  stream:
    query: SELECT * FROM users WHERE status = 'active' AND id = auth.user_id()
"#;

    assert_plan_parity(yaml);
}

#[test]
fn parity_with_js_for_response_9_join_variant() {
    let yaml = r#"
config:
  edition: 2
  sync_config_compiler: true
streams:
  stream:
    query: |
      SELECT DISTINCT u.*
      FROM public.user_organization_map AS uom1
      JOIN public.user_organization_map AS uom2 ON uom1.organization_id = uom2.organization_id
      JOIN public.users AS u ON uom2.user_id = u.id
      WHERE uom1.user_id = auth.user_id()
"#;

    assert_plan_parity(yaml);
}

#[test]
fn parity_with_js_for_response_9_subquery_variant() {
    let yaml = r#"
config:
  edition: 2
  sync_config_compiler: true
streams:
  stream:
    query: |
      SELECT
          u.*
      FROM
          public.users AS u
      JOIN
          public.user_organization_map AS uom_colleagues ON u.id = uom_colleagues.user_id
      WHERE
          uom_colleagues.organization_id IN (
              SELECT organization_id
              FROM public.user_organization_map
              WHERE user_id = auth.user_id()
          )
"#;

    assert_plan_parity(yaml);
}

#[test]
fn parity_with_js_for_response_10_nested_chain() {
    let yaml = r#"
config:
  edition: 2
  sync_config_compiler: true
streams:
  stream:
    query: |
      SELECT
          a.*
      FROM
          public.users AS u
      JOIN
          public.addresses AS a ON u.address_id = a.id
      WHERE
          u.id IN (
              SELECT
                  DISTINCT uom.user_id
              FROM
                  public.user_organization_map AS uom
              WHERE
                  uom.organization_id IN (
                      SELECT
                          organization_id
                      FROM
                          public.user_organization_map
                      WHERE
                          user_id = auth.parameter('app_metadata.user_id')
                  )
          )
"#;

    assert_plan_parity(yaml);
}

#[test]
fn parity_with_js_for_response_13_deep_joins() {
    let yaml = r#"
config:
  edition: 2
  sync_config_compiler: true
streams:
  stream:
    query: |
      select
        c.*
      from user_assignment_scope uas
      join assignment a
        on a.id = uas.assignment_id
      join assignment_checkpoint ac
        on ac.assignment_id = a.id
      join checkpoint c
        on c.id = ac.checkpoint_id
      where uas.user_id = auth.user_id()
        and a.active = true
"#;

    assert_plan_parity(yaml);
}

#[test]
fn parity_with_js_for_cte_shorthand_in_reference() {
    let yaml = r#"
config:
  edition: 2
  sync_config_compiler: true
streams:
  stream:
    auto_subscribe: true
    with:
      org_of_user: |
        SELECT id FROM organisations
          WHERE id IN (SELECT org_id FROM org_memberships WHERE user_id = auth.user_id())
    query: SELECT * FROM projects WHERE org_id IN org_of_user
"#;

    assert_plan_parity(yaml);
}

#[test]
fn parity_with_js_for_reuse_between_streams_with_different_outputs() {
    let yaml = r#"
config:
  edition: 2
  sync_config_compiler: true
streams:
  a:
    query: SELECT id, foo FROM profiles WHERE "user" = auth.user_id()
  b:
    query: SELECT id, bar FROM profiles WHERE "user" IN (SELECT member FROM orgs WHERE id = auth.parameter('org'))
"#;

    assert_plan_parity(yaml);
}

#[test]
fn parity_with_js_for_no_reuse_with_different_stream_sources() {
    let yaml = r#"
config:
  edition: 2
  sync_config_compiler: true
streams:
  a:
    query: SELECT * FROM products
  b:
    queries:
      - SELECT * FROM stores
      - SELECT * FROM products
"#;

    assert_plan_parity(yaml);
}

fn assert_plan_parity(yaml: &str) {
    let js = js_compile(yaml);
    let rust = compile_sync_plan(yaml, CompileOptions::default()).unwrap();

    let mut js_plan = js["plan"].clone();
    let mut rust_plan = serde_json::to_value(rust.plan).unwrap();

    zero_hashes(&mut js_plan);
    zero_hashes(&mut rust_plan);

    assert_eq!(rust_plan, js_plan);
}

fn js_compile(yaml: &str) -> Value {
    let script = format!(
        r#"
import {{ SqlSyncRules, serializeSyncPlan }} from './packages/sync-rules/dist/index.js';
const yaml = {yaml};
const {{config}} = SqlSyncRules.fromYaml(yaml, {{defaultSchema:'test_schema',throwOnError:true,allowNewSyncCompiler:true}});
console.log(JSON.stringify({{ plan: serializeSyncPlan(config.plan) }}));
"#,
        yaml = serde_json::to_string(yaml).unwrap()
    );

    let output = Command::new("node")
        .current_dir("../..")
        .arg("--input-type=module")
        .arg("-e")
        .arg(script)
        .output()
        .expect("node should run");

    if !output.status.success() {
        panic!(
            "node failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    serde_json::from_slice(&output.stdout).unwrap()
}

fn zero_hashes(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for (key, entry) in map.iter_mut() {
                if key == "hash" {
                    *entry = Value::Number(0.into());
                } else {
                    zero_hashes(entry);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                zero_hashes(item);
            }
        }
        _ => {}
    }
}
