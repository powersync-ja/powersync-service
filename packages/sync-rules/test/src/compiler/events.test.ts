import * as sqlite from 'node:sqlite';
import { describe, expect, test } from 'vitest';
import {
  DEFAULT_HYDRATION_STATE,
  deserializeSyncPlan,
  nodeSqlite,
  PrecompiledSyncConfig,
  serializedEventSourceDefinitionEquality,
  serializedEventSourceDefinitionIdentity,
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
    expect(deserializeSyncPlan(JSON.parse(JSON.stringify(serialized))).events).toEqual(compiled.plan.events);

    const hydrated = compiled.hydrate({ hydrationState: DEFAULT_HYDRATION_STATE, sqlite: nodeSqlite(sqlite) });
    const event = hydrated.eventDescriptors[0];
    const checkpoints = new TestSourceTable('checkpoints');

    expect(
      event.evaluateRowWithErrors({
        sourceTable: checkpoints,
        record: { user_id: 'user-1', checkpoint: 42n, client_id: 'client-1', active: 1, ignored: 'value' }
      })
    ).toEqual({
      result: { data: { user_id: 'user-1', checkpoint: 42n, client_id: 'client-1' } },
      errors: []
    });
    expect(
      event.evaluateRowWithErrors({
        sourceTable: checkpoints,
        record: { user_id: 'user-1', checkpoint: 42n, client_id: 'client-1', active: 0 }
      })
    ).toEqual({ errors: [] });
  });

  test('uses canonical semantic identity independent of formatting and filter order', () => {
    const first = eventSourceFromQuery(
      'SELECT user_id, checkpoint FROM checkpoints WHERE active = true AND checkpoint > 0'
    );
    const equivalent = eventSourceFromQuery(
      ' select user_id, checkpoint from test_schema.checkpoints AS c where c.checkpoint > 0 and c.active = true '
    );
    const changed = eventSourceFromQuery(
      'SELECT user_id, checkpoint FROM checkpoints WHERE active = true AND checkpoint > 1'
    );

    expect(serializedEventSourceDefinitionEquality.equals(first, equivalent)).toBe(true);
    expect(serializedEventSourceDefinitionIdentity(first)).toBe(serializedEventSourceDefinitionIdentity(equivalent));
    expect(serializedEventSourceDefinitionEquality.equals(first, changed)).toBe(false);
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

  test('requires payload queries within an event to use unique source tables', () => {
    const [errors] = yamlToSyncPlan(
      yamlWithEventQueries(
        'SELECT user_id FROM checkpoints',
        'SELECT checkpoint FROM test_schema.checkpoints WHERE checkpoint > 0'
      ),
      { defaultSchema: 'test_schema', throwOnError: false }
    );

    expect(errors.map((error) => error.message)).toContain('Each payload query should query a unique table');
  });
});

function eventSourceFromQuery(query: string) {
  const plan = serializeSyncPlan(
    (
      SqlSyncRules.fromYaml(yamlWithEventQueries(query), {
        defaultSchema: 'test_schema'
      }).config as PrecompiledSyncConfig
    ).plan
  );
  const source = plan.events![0].sourceQueries[0];

  return { eventName: 'write_checkpoints', defaultSchema: 'test_schema', source };
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
