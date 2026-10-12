import { describe, expect, test } from 'vitest';
import * as YAML from 'yaml';
import {
  compileSyncRulesSchemaValidator,
  createSyncRulesSchema,
  hasMongoFilterExpressions,
  MONGO_FILTER_VALIDATOR,
  parseMongoFilterExpression,
  type SourceConnectionMap,
  SqlSyncRules
} from '../../src/index.js';

function config(expression: unknown, table = 'orders/~1') {
  return YAML.stringify({
    config: { edition: 3, source_table_options: { [table]: { mongodb_filter_expression: expression } } },
    streams: { orders: { query: 'SELECT * FROM orders' } }
  });
}

describe('MongoDB expression validation', () => {
  test.each([null, false, 'invalid', []])('locates invalid option maps: %j', (value) => {
    for (const tableOptions of [false, true]) {
      const yaml = YAML.stringify({
        config: { edition: 3, source_table_options: tableOptions ? { orders: value } : value },
        streams: { orders: { query: 'SELECT * FROM orders' } }
      });
      const errors = SqlSyncRules.validate(yaml, { defaultSchema: 'app' });
      expect(errors).toHaveLength(1);
      expect(errors[0].message).toBe(
        tableOptions
          ? 'Options for a source table must be a map.'
          : 'Source-table options must be a map of table names or patterns to option maps.'
      );
      expect(YAML.parse(yaml.slice(errors[0].location.start, errors[0].location.end))).toEqual(value);
    }
  });
  test('reports independent nested errors with exact YAML locations', () => {
    const yaml = config({ $and: [{ $or: [{ $eq: [123, true] }, { $in: ['$$doc.a', []] }] }, { $or: [false] }] });
    const errors = SqlSyncRules.validate(yaml, { defaultSchema: 'app' });
    expect(errors).toHaveLength(3);
    expect(errors.map((error) => YAML.parse(yaml.slice(error.location.start, error.location.end)))).toEqual([
      123,
      [],
      false
    ]);
  });
  test('unknown operator highlights its key, not the operand', () => {
    const yaml = config({ $and: [{ $gt: ['$$doc.a', 1] }] });
    const [error] = SqlSyncRules.validate(yaml, { defaultSchema: 'app' });
    expect(yaml.slice(error.location.start, error.location.end).trim()).toBe('$gt');
  });
  test.each(['$and', '$or'])('%s rejects nonexpressions and empty operands', (operator) => {
    for (const operand of [true, [], [null], [true], [{ $eq: [123, true] }]]) {
      expect(MONGO_FILTER_VALIDATOR.safeParse({ [operator]: operand }).success).toBe(false);
    }
  });
  test('resolves nested unexpected keys and returns YAML errors', () => {
    const expression = { $eq: ['$$doc.a', { $timestamp: { t: 1, i: 2, extra: true } }] };
    const basePath = ['config', 'source_table_options', 'orders', 'mongodb_filter_expression'];
    const errors = parseMongoFilterExpression({
      value: expression,
      basePath,
      sourceLocationResolver: {
        getLocation(path, target) {
          expect(path).toEqual([...basePath, '$eq', 1, '$timestamp', 'extra']);
          expect(target).toBe('key');
          return { start: 12, end: 17 };
        }
      }
    });
    expect(errors).toHaveLength(1);
    expect(errors[0].location).toEqual({ start: 12, end: 17 });
  });
  test('editor schema has recursive references and matches valid nested input', () => {
    const schema = createSyncRulesSchema();
    const validate = compileSyncRulesSchemaValidator(schema);
    expect(
      validate(YAML.parse(config({ $and: [{ $eq: ['$$doc.active', true] }, { $or: [{ $in: ['$$doc.a', [1]] }] }] })))
    ).toBe(true);
    expect(validate(YAML.parse(config({ $eq: ['$$doc.a', { $and: [] }] })))).toBe(false);
    expect(validate(YAML.parse(config({ $eq: ['$$doc.a'] })))).toBe(false);
    expect(validate(YAML.parse(config({ $eq: ['$$doc.a', true, false] })))).toBe(false);
    expect(JSON.stringify(schema)).toContain('examples');
  });
  test('requires support for configured wildcard expressions despite disabled overrides', () => {
    expect(
      hasMongoFilterExpressions(
        {
          'orders%': { mongodb_filter_expression: { $eq: ['$$doc.a', true] } },
          orders: { mongodb_filter_expression: 'disabled' }
        },
        'default'
      )
    ).toBe(true);
    expect(
      hasMongoFilterExpressions(
        { 'other.app.orders': { mongodb_filter_expression: { $eq: ['$$doc.a', true] } } },
        'default'
      )
    ).toBe(false);
  });
});

describe('MongoDB filter connection types', () => {
  const EXPRESSION = { $eq: ['$$doc.active', true] };

  test.each(['orders', 'app.orders', 'default.app.orders', 'orders%'])(
    'checks the default connection for %s',
    (table) => {
      const yaml = config(EXPRESSION, table);
      const options = { defaultSchema: 'app', connections: { default: { type: 'mongodb' } } };
      expect(SqlSyncRules.fromYaml(yaml, options).errors).toEqual([]);
    }
  );

  test.each(['postgresql', 'mysql', 'mssql', 'convex'])(
    'rejects filters on %s sources with a YAML location',
    (type) => {
      const yaml = config(EXPRESSION, 'orders');
      const options = { defaultSchema: 'app', connections: { default: { type } } };
      expect(() => SqlSyncRules.fromYaml(yaml, options)).toThrow('only supported by MongoDB sources');
      const { errors } = SqlSyncRules.fromYaml(yaml, { ...options, throwOnError: false });
      expect(errors).toHaveLength(1);
      expect(errors[0].type).toBe('fatal');
      expect(errors[0].message).toContain(`Connection "default" has type "${type}".`);
      expect(YAML.parse(yaml.slice(errors[0].location.start, errors[0].location.end))).toEqual(EXPRESSION);
    }
  );

  test.each(['mongo.app.orders', '"Mongo".app.orders'])(
    'rejects non-default filter connections for %s even with connection metadata',
    (table) => {
      const tag = table.startsWith('"') ? 'Mongo' : 'mongo';
      const yaml = config(EXPRESSION, table);
      const connectionMaps: Array<SourceConnectionMap | undefined> = [
        undefined,
        {},
        { default: { type: 'mongodb' }, [tag]: { type: 'mongodb' } }
      ];
      for (const connections of connectionMaps) {
        const options = { defaultSchema: 'app', connections };
        expect(() => SqlSyncRules.fromYaml(yaml, options)).toThrow('only supports the "default" connection');
        const { errors } = SqlSyncRules.fromYaml(yaml, { ...options, throwOnError: false });
        expect(errors).toHaveLength(1);
        expect(errors[0].type).toBe('fatal');
        expect(YAML.parse(yaml.slice(errors[0].location.start, errors[0].location.end))).toEqual(EXPRESSION);
      }
    }
  );

  test('missing metadata produces an advisory warning at the expression', () => {
    const yaml = config(EXPRESSION, 'orders');
    const { errors } = SqlSyncRules.fromYaml(yaml, { defaultSchema: 'app' });
    expect(errors).toHaveLength(1);
    expect(errors[0].type).toBe('warning');
    expect(errors[0].message).toBe(
      'Cannot verify MongoDB replication pre-filtering for connection "default" without connection type information.'
    );
    expect(YAML.parse(yaml.slice(errors[0].location.start, errors[0].location.end))).toEqual(EXPRESSION);
  });

  test('disabled filtering does not require connection metadata or MongoDB', () => {
    const yaml = config('disabled', 'orders');
    expect(SqlSyncRules.fromYaml(yaml, { defaultSchema: 'app' }).errors).toEqual([]);
    expect(
      SqlSyncRules.fromYaml(yaml, {
        defaultSchema: 'app',
        connections: { default: { type: 'postgresql' } }
      }).errors
    ).toEqual([]);
  });
});
