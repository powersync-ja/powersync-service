import { describe, expect, test } from 'vitest';
import { DEFAULT_TAG, serializeSyncPlan, StaticSchema } from '../../../src/index.js';
import { compileToSyncPlanWithoutErrors, yamlToSyncPlan } from './utils.js';

describe('common table expressions', () => {
  test('as data source', () => {
    const plan = compileToSyncPlanWithoutErrors(`
config:
  edition: 3

streams:
  stream:
    auto_subscribe: true
    with:
      org_of_user: |
        SELECT id, name FROM organisations
          WHERE id IN (SELECT org_id FROM org_memberships WHERE user_id = auth.user_id())
    query: SELECT * FROM org_of_user
`);

    expect(serializeSyncPlan(plan)).toMatchSnapshot();
  });

  test('as parameter query', () => {
    const plan = compileToSyncPlanWithoutErrors(`
config:
  edition: 3

streams:
  stream:
    auto_subscribe: true
    with:
      org_of_user: |
        SELECT id, name FROM organisations
          WHERE id IN (SELECT org_id FROM org_memberships WHERE user_id = auth.user_id())
    query: SELECT * FROM projects WHERE org_id IN (SELECT id FROM org_of_user)
`);

    expect(serializeSyncPlan(plan)).toMatchSnapshot();

    const withDirectReference = compileToSyncPlanWithoutErrors(`
config:
  edition: 3

streams:
  stream:
    auto_subscribe: true
    with:
      org_of_user: |
        SELECT id FROM organisations
          WHERE id IN (SELECT org_id FROM org_memberships WHERE user_id = auth.user_id())
    query: SELECT * FROM projects WHERE org_id IN org_of_user
`);
    expect(serializeSyncPlan(withDirectReference)).toStrictEqual(serializeSyncPlan(plan));
  });

  describe('errors', () => {
    test('shorthand syntax for CTE with multiple columns', () => {
      const [errors] = yamlToSyncPlan(`
config:
  edition: 3

streams:
  stream:
    with:
      org_of_user: |
        SELECT id, name FROM organisations
          WHERE id IN (SELECT org_id FROM org_memberships WHERE user_id = auth.user_id())
    query: SELECT * FROM projects WHERE org_id IN org_of_user
`);

      expect(errors).toStrictEqual([
        {
          message: 'Common-table expression must return a single column',
          source: 'org_of_user'
        }
      ]);
    });
  });

  test('multiple output references', () => {
    // Regression test for https://github.com/powersync-ja/powersync-service/issues/546
    const plan = compileToSyncPlanWithoutErrors(`
config:
  edition: 3

streams:
  workspaces/role=user/type=individual:
    auto_subscribe: true
    with:
      workspaces_param: |
        SELECT value ->> 'id' AS id
        FROM json_each(auth.parameter('workspaces'))
        WHERE value ->> 'role' = 'user'
          AND value ->> 'type' = 'individual'
    queries:
      - SELECT *
        FROM workspace
        WHERE workspace.id IN workspaces_param
`);

    expect(serializeSyncPlan(plan)).toMatchSnapshot();
  });

  test('can reference quoted names from subqueries', () => {
    // Regression test for https://discord.com/channels/1138230179878154300/1422138173907144724/1479119316573094042.
    // We just want this to compile without errors.
    compileToSyncPlanWithoutErrors(`
config:
  edition: 3
streams:
  migrated_to_streams:
    auto_subscribe: true
    with:
      user_items_param: SELECT "orgId" FROM "OrgMember" WHERE "userId" = auth.user_id()
    queries:
      - SELECT "Project".* FROM "Project", user_items_param AS bucket WHERE "Project"."orgId" = bucket."orgId"
`);
  });

  describe('global', () => {
    test('can be used', () => {
      const compiled = compileToSyncPlanWithoutErrors(`
config:
  edition: 3

with:
  owned_orgs: SELECT id FROM orgs WHERE owner = auth.user_id()

streams:
  orgs:
    query: SELECT * FROM orgs WHERE id IN owned_orgs
  notes:
    query: SELECT notes.* FROM notes INNER JOIN owned_orgs ON owned_orgs.id = notes.org_id
`);

      expect(compiled.parameterIndexes).toHaveLength(1);
    });

    test('local ctes take precedence', () => {
      compileToSyncPlanWithoutErrors(`
config:
  edition: 3

with:
  owned_orgs: SELECT id FROM orgs WHERE owner = auth.user_id()

streams:
  orgs:
    with:
      owned_orgs: SELECT id AS org_id FROM orgs WHERE owner = auth.user_id()
    # This would emit an error about a missing org_id column if the global definition was used.
    query: SELECT * FROM orgs WHERE id IN (SELECT org_id FROM owned_orgs)
`);
    });
  });

  test('warns if CTE matches table name', () => {
    const schema = new StaticSchema([
      {
        tag: DEFAULT_TAG,
        schemas: [
          {
            name: 'test_schema',
            tables: [
              {
                name: 'orgs',
                columns: [
                  { name: 'id', sqlite_type: 'text', internal_type: 'uuid' },
                  { name: 'owner', sqlite_type: 'text', internal_type: 'uuid' }
                ]
              },
              {
                name: 'owned_orgs',
                columns: [
                  { name: 'id', sqlite_type: 'text', internal_type: 'uuid' },
                  { name: 'owner', sqlite_type: 'text', internal_type: 'uuid' }
                ]
              }
            ]
          }
        ]
      }
    ]);

    const [errors] = yamlToSyncPlan(
      `
config:
  edition: 3

with:
  owned_orgs: SELECT id FROM orgs WHERE owner = auth.user_id()

streams:
  orgs:
    query: SELECT * FROM orgs WHERE id IN owned_orgs
`,
      { defaultSchema: 'test_schema', schema }
    );

    expect(errors).toStrictEqual([
      {
        isWarning: true,
        message: 'This common table expression shadows the name of a table in the source schema.',
        source: 'owned_orgs'
      }
    ]);
  });

  test('can reference CTE multiple times', () => {
    const plan = compileToSyncPlanWithoutErrors(`
config:
  edition: 3

streams:
  a:
    accept_potentially_dangerous_queries: true
    with:
      selected_profile: |
        SELECT id FROM user_profiles WHERE id = auth.user_id()
    queries:
      - SELECT * FROM tbl_a
        WHERE col_1 IN selected_profile
          AND col_2 IN (
            SELECT id FROM tbl_2
            WHERE col_1 IN selected_profile
          )
`);

    expect(serializeSyncPlan(plan)).toMatchSnapshot();
  });
});
