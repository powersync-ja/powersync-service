import { describe, expect, test } from 'vitest';
import {
  ColumnDefinition,
  sqlTypeName,
  CompatibilityContext,
  DEFAULT_TAG,
  javaScriptExpressionEngine,
  SourceTableDefinition,
  StaticSchema,
  TablePattern,
  PrecompiledSyncConfig,
  deserializeSyncPlan,
  ExpressionType
} from '../../../src/index.js';
import { compileSingleStreamAndSerialize } from '../compiler/utils.js';
import { SyncPlanSchemaAnalyzer } from '../../../src/sync_plan/schema_inference.js';

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

describe('schema inference', () => {
  function generateSchema(...queries: string[]) {
    const serializedPlan = compileSingleStreamAndSerialize(...queries);
    const plan = deserializeSyncPlan(serializedPlan);
    const rules = new PrecompiledSyncConfig(plan, new CompatibilityContext({ edition: 3 }), [], {
      // Engine isn't actually used here, but required to load sync plan
      engine: javaScriptExpressionEngine(CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY),
      sourceText: '',
      defaultSchema: 'test_schema'
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

describe('parameter inference', () => {
  function inferParameters(...queries: string[]) {
    const serializedPlan = compileSingleStreamAndSerialize(...queries);
    const plan = deserializeSyncPlan(serializedPlan);

    const analyzer = new SyncPlanSchemaAnalyzer('test_schema', schema);
    return analyzer.resolveReferencedParameters(plan.streams[0].queriers);
  }

  test('group by text', () => {
    expect(inferParameters(`SELECT * FROM assets WHERE name = subscription.parameter('name')`)).toStrictEqual({
      name: {
        originalType: 'text',
        type: ExpressionType.TEXT
      }
    });
  });

  test('group by int', () => {
    expect(inferParameters(`SELECT * FROM assets WHERE count = subscription.parameter('count')`)).toStrictEqual({
      count: {
        originalType: 'int4',
        type: ExpressionType.INTEGER
      }
    });
  });

  test('custom filter', () => {
    // We don't support inferring parameter types of functions at the moment.
    expect(inferParameters(`SELECT * FROM assets WHERE name = UPPER(subscription.parameter('name'))`)).toStrictEqual({
      name: {
        type: ExpressionType.ANY
      }
    });
  });

  test('static filter', () => {
    // We don't support inferring parameter types of functions at the moment.
    expect(inferParameters(`SELECT * FROM assets WHERE subscription.parameter('include_assets')`)).toStrictEqual({
      include_assets: {
        originalType: 'bool',
        type: ExpressionType.INTEGER
      }
    });
  });

  test('parameter lookup', () => {
    // We don't support inferring parameter types of functions at the moment.
    expect(
      inferParameters(
        `SELECT * FROM assets WHERE name IN (SELECT name FROM assets WHERE owner_id = subscription.parameter('owner'))`
      )
    ).toStrictEqual({
      owner: {
        originalType: 'uuid',
        type: ExpressionType.TEXT
      }
    });
  });

  test('json_each input', () => {
    expect(inferParameters(`SELECT * FROM assets WHERE name IN subscription.parameter('names')`)).toStrictEqual({
      names: {
        type: ExpressionType.TEXT
      }
    });
  });
});
