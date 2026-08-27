import * as sqlite from 'node:sqlite';
import { describe, expect, test } from 'vitest';
import { StableHasher } from '../../../src/compiler/equality.js';
import {
  DEFAULT_HYDRATION_STATE,
  deserializeSyncPlan,
  nodeSqlite,
  PrecompiledSyncConfig,
  serializedEventDefinitionEquality,
  serializeSyncPlan,
  SqlSyncRules
} from '../../../src/index.js';
import { TestSourceTable } from '../util.js';
import { yamlToSyncPlan } from './utils.js';

const CHECKPOINT_EVENT_YAML = `
config:
  edition: 3

streams:
  checkpoints:
    query: SELECT * FROM checkpoints

event_definitions:
  write_checkpoints:
    payloads:
      - SELECT user_id, checkpoint, client_id FROM checkpoints WHERE active = true
`;

describe('compiled replication events', () => {
  test('compiles, serializes and evaluates event payload queries', () => {
    const { config, errors } = SqlSyncRules.fromYaml(CHECKPOINT_EVENT_YAML, {
      defaultSchema: 'test_schema',
      throwOnError: false
    });
    expect(errors).toStrictEqual([]);
    expect(config).toBeInstanceOf(PrecompiledSyncConfig);

    const compiled = config as PrecompiledSyncConfig;
    const serialized = serializeSyncPlan(compiled.plan);
    // Compiled events are additive and do not require a new plan version.
    expect(serialized.version).toBe(1);
    expect(serialized.events).toHaveLength(1);
    const deserialized = deserializeSyncPlan(JSON.parse(JSON.stringify(serialized)));
    expect(deserialized.events).toMatchObject(compiled.plan.events);

    const hydrated = compiled.hydrate({ hydrationState: DEFAULT_HYDRATION_STATE, sqlite: nodeSqlite(sqlite) });
    const event = hydrated.eventDescriptors[0];
    expect(event.name).toBe('write_checkpoints');
    const checkpoints = new TestSourceTable('checkpoints');

    expect(
      event.evaluateRowWithErrors({
        sourceTable: checkpoints,
        record: { user_id: 'user-1', checkpoint: 42n, client_id: 'client-1', active: 1, ignored: 'value' }
      })
    ).toEqual({
      results: [{ data: { user_id: 'user-1', checkpoint: 42n, client_id: 'client-1' } }],
      errors: []
    });
    expect(
      event.evaluateRowWithErrors({
        sourceTable: checkpoints,
        record: { user_id: 'user-1', checkpoint: 42n, client_id: 'client-1', active: 0 }
      })
    ).toEqual({ results: [], errors: [] });
  });

  /**
   * Editions without persisted compiled plans must retain the legacy behavior across deploys and restarts. Their event
   * payload filters are ignored with a warning instead of being newly validated or applied during a service upgrade.
   */
  test('preserves legacy event payload filter behavior before edition 3', () => {
    const { config, errors } = SqlSyncRules.fromYaml(
      `
bucket_definitions:
  checkpoints:
    data:
      - SELECT * FROM checkpoints

event_definitions:
  write_checkpoints:
    payloads:
      - SELECT user_id FROM checkpoints WHERE user_id = request.user_id()
`,
      { defaultSchema: 'test_schema', throwOnError: true }
    );

    expect(errors).toHaveLength(1);
    expect(errors[0]).toMatchObject({ type: 'warning' });
    expect(errors[0].message).toContain('legacy event payload queries are ignored');

    const event = config.hydrate({ hydrationState: DEFAULT_HYDRATION_STATE, sqlite: nodeSqlite(sqlite) })
      .eventDescriptors[0];
    expect(
      event.evaluateRowWithErrors({
        sourceTable: new TestSourceTable('checkpoints'),
        record: { user_id: 'user-1' }
      })
    ).toEqual({ results: [{ data: { user_id: 'user-1' } }], errors: [] });
  });

  test('returns payloads from every matching source query', () => {
    const { config, errors } = SqlSyncRules.fromYaml(
      yamlWithEventQueries(
        `SELECT 'wildcard' AS kind, checkpoint FROM "checkpoints_%"`,
        `SELECT 'exact' AS kind, checkpoint FROM checkpoints_2026`,
        `SELECT 'second-exact' AS kind, checkpoint FROM test_schema.checkpoints_2026 WHERE checkpoint > 0`
      ),
      { defaultSchema: 'test_schema', throwOnError: false }
    );
    expect(errors).toEqual([]);

    const event = config.hydrate({ hydrationState: DEFAULT_HYDRATION_STATE, sqlite: nodeSqlite(sqlite) })
      .eventDescriptors[0];
    const evaluated = event.evaluateRowWithErrors({
      sourceTable: new TestSourceTable('checkpoints_2026'),
      record: { checkpoint: 42n }
    });
    expect(evaluated.errors).toEqual([]);
    expect(evaluated.results).toHaveLength(3);
    expect(evaluated.results).toEqual(
      expect.arrayContaining([
        { data: { kind: 'wildcard', checkpoint: 42n } },
        { data: { kind: 'exact', checkpoint: 42n } },
        { data: { kind: 'second-exact', checkpoint: 42n } }
      ])
    );
  });

  // Persisted equality ignores representation-only differences while retaining every field that affects event
  // evaluation and payloads.
  test.each([
    {
      description: 'formatting and payload-query order',
      first: [
        'SELECT user_id, checkpoint FROM checkpoints WHERE active = true AND checkpoint > 0',
        'SELECT user_id, checkpoint FROM archived_checkpoints'
      ],
      second: [
        ' select "user_id", "checkpoint" from "archived_checkpoints" ',
        'select "user_id", "checkpoint" from "checkpoints" where "active" = true and "checkpoint" > 0'
      ],
      equal: true
    },
    {
      description: 'source aliases',
      first: ['SELECT checkpoint.user_id FROM checkpoints AS checkpoint WHERE checkpoint.active = true'],
      second: ['SELECT source_row.user_id FROM checkpoints AS source_row WHERE source_row.active = true'],
      equal: true
    },
    {
      description: 'projected payload aliases',
      first: ['SELECT user_id AS payload_user FROM checkpoints'],
      second: ['SELECT user_id AS account FROM checkpoints'],
      equal: false
    },
    {
      description: 'external-data bindings',
      first: ['SELECT user_id FROM checkpoints WHERE active = true'],
      second: ['SELECT user_id FROM checkpoints WHERE archived = true'],
      equal: false
    },
    {
      description: 'filter literals',
      first: ['SELECT user_id FROM checkpoints WHERE checkpoint > 0'],
      second: ['SELECT user_id FROM checkpoints WHERE checkpoint > 1'],
      equal: false
    },
    {
      description: 'reordered expression operands',
      first: ['SELECT user_id FROM checkpoints WHERE active = true'],
      second: ['SELECT user_id FROM checkpoints WHERE true = active'],
      equal: false
    },
    {
      description: 'reordered filter clauses',
      first: ['SELECT user_id FROM checkpoints WHERE active = true AND checkpoint > 0'],
      second: ['SELECT user_id FROM checkpoints WHERE checkpoint > 0 AND active = true'],
      equal: false
    },
    {
      description: 'reordered projected columns',
      first: ['SELECT user_id, checkpoint FROM checkpoints'],
      second: ['SELECT checkpoint, user_id FROM checkpoints'],
      equal: false
    }
  ])('compares serialized event behavior for $description', ({ first, second, equal }) => {
    const firstSerialized = serializedEventDefinitionFromQueries(...first);
    const secondSerialized = serializedEventDefinitionFromQueries(...second);

    expect(serializedEventDefinitionEquality.equals(firstSerialized, secondSerialized)).toBe(equal);
    if (equal) {
      expect(StableHasher.hashWith(serializedEventDefinitionEquality, firstSerialized)).toEqual(
        StableHasher.hashWith(serializedEventDefinitionEquality, secondSerialized)
      );
    }
  });

  // Raw SQL and cached evaluator hashes are persisted metadata, not event behavior.
  test('excludes serialized compatibility metadata from equality', () => {
    const original = serializedEventDefinitionFromQueries(
      'SELECT user_id, checkpoint FROM checkpoints WHERE active = true'
    );
    const metadataChanged = structuredClone(original);
    metadataChanged.sourceQueries[0].sql = 'raw sql is compatibility metadata only';
    metadataChanged.sourceQueries[0].variants[0].hash++;

    expect(serializedEventDefinitionEquality.equals(original, metadataChanged)).toBe(true);
    expect(StableHasher.hashWith(serializedEventDefinitionEquality, original)).toEqual(
      StableHasher.hashWith(serializedEventDefinitionEquality, metadataChanged)
    );
  });

  test('treats MongoDB-normalized optional expression fields as equal', () => {
    const original = serializedEventDefinitionFromQueries(
      'SELECT CASE WHEN checkpoint_requested_at IS NULL THEN true ELSE false END AS is_legacy FROM checkpoints'
    );
    const persisted = structuredClone(original);
    const expression = persisted.sourceQueries[0].variants[0].columns[0];
    expect(expression).not.toBe('star');
    if (expression == 'star' || expression.expr.type != 'case_when') {
      throw new Error('Expected the event payload to compile to a CASE expression');
    }
    (expression.expr as { operand?: unknown }).operand = null;

    expect(serializedEventDefinitionEquality.equals(original, persisted)).toBe(true);
    expect(StableHasher.hashWith(serializedEventDefinitionEquality, original)).toEqual(
      StableHasher.hashWith(serializedEventDefinitionEquality, persisted)
    );
  });

  // Persisted equality must compare the evaluator that produced existing data rather than recompiling its retained SQL.
  test('detects changed serialized evaluator behavior when raw SQL is unchanged', () => {
    const original = serializedEventDefinitionFromQueries(
      'SELECT user_id FROM checkpoints WHERE active = true AND checkpoint > 0'
    );
    const changedEvaluator = structuredClone(original);
    changedEvaluator.sourceQueries[0].variants[0].filters = [];

    expect(changedEvaluator.sourceQueries[0].sql).toEqual(original.sourceQueries[0].sql);
    expect(serializedEventDefinitionEquality.equals(original, changedEvaluator)).toBe(false);
  });

  // Event names are observable by handlers and remain part of persisted compatibility matching.
  test('compares serialized event names', () => {
    const original = serializedEventDefinitionFromQueries('SELECT user_id FROM checkpoints');
    const renamed = structuredClone(original);
    renamed.name = 'other_event';

    expect(serializedEventDefinitionEquality.equals(original, renamed)).toBe(false);
  });

  test.each([
    ['joins', 'SELECT c.user_id FROM checkpoints c JOIN users u ON u.id = c.user_id', 'single physical source table'],
    [
      'request parameters',
      'SELECT user_id FROM checkpoints WHERE user_id = auth.user_id()',
      'cannot depend on request parameters'
    ]
  ])('rejects %s', (_label, query, message) => {
    const [errors] = yamlToSyncPlan(yamlWithEventQueries(query), {
      defaultSchema: 'test_schema',
      throwOnError: false
    });

    expect(errors.map((error) => error.message)).toContainEqual(expect.stringContaining(message));
  });
});

function serializedEventDefinitionFromQueries(...queries: string[]) {
  const [errors, plan] = yamlToSyncPlan(yamlWithEventQueries(...queries), {
    defaultSchema: 'test_schema',
    throwOnError: false
  });
  expect(errors).toEqual([]);

  return serializeSyncPlan(plan).events![0];
}

function yamlWithEventQueries(...queries: string[]): string {
  return `
config:
  edition: 3

streams:
  checkpoints:
    query: SELECT * FROM checkpoints

event_definitions:
  write_checkpoints:
    payloads:
${queries.map((query) => `      - ${query}`).join('\n')}
`;
}
