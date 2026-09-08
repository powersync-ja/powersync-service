import { describe, expect, test } from 'vitest';
import { compileToSyncPlanWithoutErrors } from './utils.js';

describe('can reuse elements', () => {
  test('multiple data queries', () => {
    const compiled = compileToSyncPlanWithoutErrors(`
config:
  edition: 3

streams:
  stream:
    queries:
      - SELECT * FROM products
      - SELECT * FROM stores      
`);

    // Products and stores should go in same bucket.
    expect(compiled.buckets).toHaveLength(1);
  });

  test('between streams', () => {
    const compiled = compileToSyncPlanWithoutErrors(`
config:
  edition: 3

streams:
  a:
    query: SELECT * FROM profiles WHERE "user" = auth.user_id()
  b:
    query: SELECT * FROM profiles WHERE "user" IN (SELECT member FROM orgs WHERE id = auth.parameter('org'))
`);

    // There is only one bucket, profiles partitioned by profiles.user
    expect(compiled.buckets).toHaveLength(1);
  });

  test('between streams with different outputs', () => {
    const compiled = compileToSyncPlanWithoutErrors(`
config:
  edition: 3

streams:
  a:
    query: SELECT id, foo FROM profiles WHERE "user" = auth.user_id()
  b:
    query: SELECT id, bar FROM profiles WHERE "user" IN (SELECT member FROM orgs WHERE id = auth.parameter('org'))
`);

    expect(compiled.buckets).toHaveLength(2);
  });

  test('no reuse on streams with different sources', () => {
    const compiled = compileToSyncPlanWithoutErrors(`
config:
  edition: 3

streams:
  a:
    query: SELECT * FROM products
  b:
    queries:
      - SELECT * FROM stores
      - SELECT * FROM products
`);

    expect(compiled.buckets).toHaveLength(2);
    expect(compiled.dataSources).toHaveLength(2);
  });

  test('parameters between streams', () => {
    const compiled = compileToSyncPlanWithoutErrors(`
config:
  edition: 3

streams:
  a:
    query: SELECT * FROM projects WHERE org IN (SELECT org FROM users WHERE id = auth.user_id())
  b:
    queries:
      - SELECT * FROM subscriptions WHERE org IN (SELECT org FROM users WHERE id = auth.user_id())
`);

    expect(compiled.parameterIndexes).toHaveLength(1);
  });

  test('reuse with filter referencing a table-valued function column', () => {
    const compiled = compileToSyncPlanWithoutErrors(`
config:
  edition: 3

streams:
  a:
    query: SELECT posts.* FROM posts, json_each(posts.a) AS ja WHERE ja.value = 'x'
  b:
    query: SELECT posts.* FROM posts, json_each(posts.a) AS ja WHERE ja.value = 'x'
`);

    expect(compiled.dataSources).toHaveLength(1);
  });

  test('point lookup with same-named output columns from two different table-valued functions', () => {
    const compiled = compileToSyncPlanWithoutErrors(`
config:
  edition: 3

streams:
  a:
    query: SELECT users.* FROM users, orgs, json_each(orgs.a) as ja, json_each(orgs.b) as jb WHERE users.x = ja.value AND users.y = jb.value AND orgs.id = auth.parameter('org')
`);

    expect(compiled.parameterIndexes).toHaveLength(1);
  });
});
