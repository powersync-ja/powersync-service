import { describe, expect, test } from 'vitest';
import { compileToSyncPlanWithoutErrors, yamlToSyncPlan } from './utils.js';
import { serializeSyncPlan } from '../../../src/index.js';

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
});
