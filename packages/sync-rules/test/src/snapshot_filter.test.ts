// import { describe, expect, test } from 'vitest';
// import { SqlSyncRules } from '../../src/index.js';
// import { PARSE_OPTIONS } from './util.js';

// describe('snapshot filter tests', () => {
//   test('parse initial_snapshot_filter from global config', () => {
//     const { config: rules } = SqlSyncRules.fromYaml(
//       `
// initial_snapshot_filters:
//   users:
//     sql: "status = 'active'"

// bucket_definitions:
//   active_users:
//     data:
//       - SELECT id, name FROM users
//     `,
//       PARSE_OPTIONS
//     );

//     expect(rules.initialSnapshotFilters.size).toBe(1);
//     expect(rules.initialSnapshotFilters.get('users')).toEqual({
//       sql: "status = 'active'"
//     });
//   });

//   test('parse multiple tables with global filters', () => {
//     const { config: rules } = SqlSyncRules.fromYaml(
//       `
// initial_snapshot_filters:
//   users:
//     sql: "status = 'active'"
//   orders:
//     sql: "created_at > DATE_SUB(NOW(), INTERVAL 90 DAY)"

// bucket_definitions:
//   active_users:
//     data:
//       - SELECT id, name FROM users WHERE status = 'active'
  
//   recent_orders:
//     data:
//       - SELECT * FROM orders
//     `,
//       PARSE_OPTIONS
//     );

//     expect(rules.initialSnapshotFilters.size).toBe(2);
//     expect(rules.initialSnapshotFilters.get('users')).toEqual({ sql: "status = 'active'" });
//     expect(rules.initialSnapshotFilters.get('orders')).toEqual({
//       sql: 'created_at > DATE_SUB(NOW(), INTERVAL 90 DAY)'
//     });
//   });

//   test('getInitialSnapshotFilter returns undefined when no filters', () => {
//     const { config: rules } = SqlSyncRules.fromYaml(
//       `
// bucket_definitions:
//   all_users:
//     data:
//       - SELECT id, name FROM users
//     `,
//       PARSE_OPTIONS
//     );

//     const filter = rules.getInitialSnapshotFilter('default', 'public', 'users');
//     expect(filter).toBeUndefined();
//   });

//   test('getInitialSnapshotFilter returns single filter', () => {
//     const { config: rules } = SqlSyncRules.fromYaml(
//       `
// initial_snapshot_filters:
//   users:
//     sql: "status = 'active'"

// bucket_definitions:
//   active_users:
//     data:
//       - SELECT id, name FROM users
//     `,
//       PARSE_OPTIONS
//     );

//     const filter = rules.getInitialSnapshotFilter('default', 'public', 'users', 'sql');
//     expect(filter).toBe("status = 'active'");
//   });

//   test('getInitialSnapshotFilter handles schema-qualified table names', () => {
//     const { config: rules } = SqlSyncRules.fromYaml(
//       `
// initial_snapshot_filters:
//   public.users:
//     sql: "status = 'active'"

// bucket_definitions:
//   active_users:
//     data:
//       - SELECT id, name FROM public.users
//     `,
//       PARSE_OPTIONS
//     );

//     const filter = rules.getInitialSnapshotFilter('default', 'public', 'users', 'sql');
//     expect(filter).toBe("status = 'active'");
//   });

//   test('initial_snapshot_filter with complex WHERE clause', () => {
//     const { config: rules } = SqlSyncRules.fromYaml(
//       `
// initial_snapshot_filters:
//   todos:
//     sql: "created_at > DATE_SUB(NOW(), INTERVAL 30 DAY) AND archived = false"

// bucket_definitions:
//   recent_todos:
//     data:
//       - SELECT * FROM todos
//     `,
//       PARSE_OPTIONS
//     );

//     const filter = rules.initialSnapshotFilters.get('todos');
//     expect(filter).toEqual({
//       sql: 'created_at > DATE_SUB(NOW(), INTERVAL 30 DAY) AND archived = false'
//     });
//   });

//   test('parse succeeds without errors when using initial_snapshot_filter', () => {
//     const { config: rules, errors } = SqlSyncRules.fromYaml(
//       `
// initial_snapshot_filters:
//   users:
//     sql: "status = 'active'"

// bucket_definitions:
//   active_users:
//     data:
//       - SELECT id, name FROM users
//     `,
//       PARSE_OPTIONS
//     );

//     expect(errors).toHaveLength(0);
//     expect(rules.initialSnapshotFilters.size).toBe(1);
//   });

//   test('parse initial_snapshot_filter with object format', () => {
//     const { config: rules, errors } = SqlSyncRules.fromYaml(
//       `
// initial_snapshot_filters:
//   users:
//     sql: "status = 'active'"
//     mongo: {status: 'active'}

// bucket_definitions:
//   active_users:
//     data:
//       - SELECT id, name FROM users
//     `,
//       PARSE_OPTIONS
//     );

//     expect(errors).toHaveLength(0);
//     expect(rules.initialSnapshotFilters.size).toBe(1);
//     const filter = rules.initialSnapshotFilters.get('users');
//     expect(filter).toEqual({
//       sql: "status = 'active'",
//       mongo: { status: 'active' }
//     });
//   });

//   test('getInitialSnapshotFilter with object format - SQL and Mongo', () => {
//     const { config: rules } = SqlSyncRules.fromYaml(
//       `
// initial_snapshot_filters:
//   users:
//     sql: "status = 'active'"
//     mongo: {status: 'active'}

// bucket_definitions:
//   active_users:
//     data:
//       - SELECT id, name FROM users
//     `,
//       PARSE_OPTIONS
//     );

//     const sqlFilter = rules.getInitialSnapshotFilter('default', 'public', 'users', 'sql');
//     expect(sqlFilter).toBe("status = 'active'");

//     const mongoFilter = rules.getInitialSnapshotFilter('default', 'public', 'users', 'mongo');
//     expect(mongoFilter).toEqual({ status: 'active' });
//   });

//   test('getInitialSnapshotFilter with object format - only sql specified', () => {
//     const { config: rules } = SqlSyncRules.fromYaml(
//       `
// initial_snapshot_filters:
//   users:
//     sql: "archived = false"

// bucket_definitions:
//   active_users:
//     data:
//       - SELECT id, name FROM users
//     `,
//       PARSE_OPTIONS
//     );

//     const sqlFilter = rules.getInitialSnapshotFilter('default', 'public', 'users', 'sql');
//     expect(sqlFilter).toBe('archived = false');

//     const mongoFilter = rules.getInitialSnapshotFilter('default', 'public', 'users', 'mongo');
//     expect(mongoFilter).toBeUndefined();
//   });
// });
