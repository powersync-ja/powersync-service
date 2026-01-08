import { describe, expect, test } from 'vitest';
import { compileToSyncPlanWithoutErrors } from './utils.js';

describe('can reuse elements', () => {
  test('multiple data queries', () => {
    const compiled = compileToSyncPlanWithoutErrors([
      { name: 'stream', queries: ['SELECT * FROM products', 'SELECT * FROM stores'] }
    ]);

    // Products and stores should go in same bucket.
    expect(compiled.buckets).toHaveLength(1);
  });

  test('between streams', () => {
    const compiled = compileToSyncPlanWithoutErrors([
      { name: 'a', queries: ['SELECT * FROM profiles WHERE "user" = auth.user_id()'] },
      {
        name: 'b',
        queries: [`SELECT * FROM profiles WHERE "user" IN (SELECT member FROM orgs WHERE id = auth.parameter('org'))`]
      }
    ]);

    // There is only one bucket, profiles partitioned by profiles.user
    expect(compiled.buckets).toHaveLength(1);
  });

  test('no reuse on streams with different sources', () => {
    const compiled = compileToSyncPlanWithoutErrors([
      { name: 'a', queries: ['SELECT * FROM products'] },
      {
        name: 'b',
        queries: [`SELECT * FROM stores`, `SELECT * FROM products`]
      }
    ]);

    expect(compiled.buckets).toHaveLength(2);
    expect(compiled.dataSources).toHaveLength(2);
  });

  test('parameters between streams', () => {
    const compiled = compileToSyncPlanWithoutErrors([
      { name: 'a', queries: ['SELECT * FROM projects WHERE org IN (SELECT org FROM users WHERE id = auth.user_id())'] },
      {
        name: 'b',
        queries: ['SELECT * FROM subscriptions WHERE org IN (SELECT org FROM users WHERE id = auth.user_id())']
      }
    ]);

    expect(compiled.parameterIndexes).toHaveLength(1);
  });
});
