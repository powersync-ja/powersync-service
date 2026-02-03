import { describe, expect, test } from 'vitest';
import {
  addPrecompiledSyncPlanToRules,
  ColumnDefinition,
  sqlTypeName,
  CompatibilityContext,
  DEFAULT_TAG,
  javaScriptExpressionEngine,
  SourceTableDefinition,
  SqlSyncRules,
  StaticSchema,
  TablePattern
} from '../../../../src/index.js';
import { compileToSyncPlanWithoutErrors } from '../../compiler/utils.js';

describe('schema inference', () => {
  const assetsTable: SourceTableDefinition = {
    name: 'assets',
    columns: [
      { name: 'id', sqlite_type: 'text', internal_type: 'uuid' },
      { name: 'name', sqlite_type: 'text', internal_type: 'text' },
      { name: 'count', sqlite_type: 'integer', internal_type: 'int4' },
      { name: 'owner_id', sqlite_type: 'text', internal_type: 'uuid' }
    ]
  };
  const schema = new StaticSchema([
    {
      tag: DEFAULT_TAG,
      schemas: [
        {
          name: 'test_schema',
          tables: [assetsTable]
        }
      ]
    }
  ]);

  function generateSchema(...queries: string[]) {
    const plan = compileToSyncPlanWithoutErrors([{ name: 'stream', queries }]);
    const rules = new SqlSyncRules('');

    addPrecompiledSyncPlanToRules(plan, rules, {
      // Engine isn't actually used here, but required to load sync plan
      engine: javaScriptExpressionEngine(CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY)
    });

    const outputSchema: Record<string, Record<string, ColumnDefinition>> = {};
    for (const source of rules.bucketDataSources) {
      source.resolveResultSets(schema, outputSchema);
    }

    return outputSchema;
  }

  function typesOnly(source: Record<string, ColumnDefinition>): Record<string, string> {
    return Object.fromEntries(Object.entries(source).map(([k, v]) => [k, sqlTypeName(v)]));
  }

  test('table', () => {
    const resolvedSchema = generateSchema('SELECT * FROM assets');
    expect(Object.keys(resolvedSchema)).toStrictEqual(['assets']);

    const expectedAssets: Record<string, ColumnDefinition> = {};
    for (const column of schema.getTables(new TablePattern('test_schema', 'assets'))[0].getColumns()) {
      if (column.name != 'id') expectedAssets[column.name] = column;
    }
    expect(resolvedSchema['assets']).toStrictEqual(expectedAssets);
  });

  test('table alias', () => {
    const resolvedSchema = generateSchema(`SELECT * FROM assets public_assets`);
    expect(Object.keys(resolvedSchema)).toStrictEqual(['public_assets']);
  });

  test('table pattern', () => {
    let resolvedSchema = generateSchema(`SELECT * FROM "%"`);
    expect(Object.keys(resolvedSchema)).toStrictEqual(['assets']);

    resolvedSchema = generateSchema(`SELECT * FROM "%" AS fixed`);
    expect(Object.keys(resolvedSchema)).toStrictEqual(['fixed']);
  });

  test('literal', () => {
    const resolvedSchema = generateSchema(`SELECT id, 1 AS i, 2.5 AS d, null AS n, 'text' AS t FROM assets`).assets;
    expect(typesOnly(resolvedSchema)).toStrictEqual({
      i: 'integer',
      d: 'real',
      n: 'text', // default type for null
      t: 'text'
    });
  });

  test('cast', () => {
    const resolvedSchema = generateSchema(`SELECT id, CAST(assets.count AS TEXT) c FROM assets`).assets;
    expect(typesOnly(resolvedSchema)).toStrictEqual({
      c: 'text'
    });
  });

  test('unary', () => {
    const resolvedSchema = generateSchema(`SELECT id, NOT name AS name, +count AS count FROM assets`).assets;
    expect(typesOnly(resolvedSchema)).toStrictEqual({
      name: 'integer',
      count: 'integer'
    });
  });

  test('binary', () => {
    const resolvedSchema = generateSchema(`SELECT id, count * 2 AS c FROM assets`).assets;
    expect(typesOnly(resolvedSchema)).toStrictEqual({
      c: 'integer'
    });
  });

  test('case when', () => {
    const resolvedSchema = generateSchema(`SELECT id, CASE WHEN TRUE THEN 1 ELSE 2.5 END AS c FROM assets`).assets;
    expect(typesOnly(resolvedSchema)).toStrictEqual({
      c: 'real'
    });
  });

  test('functions', () => {
    const resolvedSchema = generateSchema(`SELECT id, hex(null) h FROM assets`).assets;
    expect(typesOnly(resolvedSchema)).toStrictEqual({
      h: 'text'
    });
  });
});
