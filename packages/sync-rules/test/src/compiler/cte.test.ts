import { describe, expect, test } from 'vitest';
import { compileToSyncPlanWithoutErrors } from './utils.js';
import { serializeSyncPlan } from '../../../src/index.js';

describe('common table expressions', () => {
  test('as data source', () => {
    const plan = compileToSyncPlanWithoutErrors([
      {
        name: 'stream',
        ctes: {
          org_of_user: `SELECT id, name FROM organisations
                WHERE id IN (SELECT org_id FROM org_memberships WHERE user_id = auth.user_id())`
        },
        queries: ['SELECT * FROM org_of_user;']
      }
    ]);

    expect(serializeSyncPlan(plan)).toMatchSnapshot();
  });

  test('as parameter query', () => {
    const plan = compileToSyncPlanWithoutErrors([
      {
        name: 'stream',
        ctes: {
          org_of_user: `SELECT id, name FROM organisations
                WHERE id IN (SELECT org_id FROM org_memberships WHERE user_id = auth.user_id())`
        },
        queries: ['SELECT * FROM projects WHERE org_id IN (SELECT id FROM org_of_user);']
      }
    ]);

    expect(serializeSyncPlan(plan)).toMatchSnapshot();
  });
});
