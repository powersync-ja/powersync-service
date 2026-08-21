import * as sqlite from 'node:sqlite';
import { describe, expect, test } from 'vitest';
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

  test('matches definitions independent of formatting, quoting and payload-query order', () => {
    const first = eventDefinitionFromQueries(
      'SELECT user_id, checkpoint FROM checkpoints WHERE active = true AND checkpoint > 0',
      'SELECT user_id, checkpoint FROM archived_checkpoints'
    );
    const equivalent = eventDefinitionFromQueries(
      ' select "user_id", "checkpoint" from "archived_checkpoints" ',
      'select "user_id", "checkpoint" from "checkpoints" where "active" = true and "checkpoint" > 0'
    );
    const changed = eventDefinitionFromQueries(
      'SELECT user_id, checkpoint FROM checkpoints WHERE active = true AND checkpoint > 1',
      'SELECT user_id, checkpoint FROM archived_checkpoints'
    );

    expect(serializedEventDefinitionEquality.equals(first, equivalent)).toBe(true);
    expect(serializedEventDefinitionEquality.equals(first, changed)).toBe(false);
  });

  test('excludes the raw SQL mirror from the identity', () => {
    const original = eventDefinitionFromQueries('SELECT user_id, checkpoint FROM checkpoints WHERE active = true');
    const differentSql = structuredClone(original);
    differentSql.sourceQueries[0].sql = 'raw sql is compatibility metadata only';

    expect(serializedEventDefinitionEquality.equals(original, differentSql)).toBe(true);
  });

  test('matches independent of the default schema used to compile', () => {
    const query = 'SELECT user_id, checkpoint FROM checkpoints WHERE active = true';

    expect(
      serializedEventDefinitionEquality.equals(
        eventDefinitionForSchema('first_schema', query),
        eventDefinitionForSchema('second_schema', query)
      )
    ).toBe(true);
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

function eventDefinitionFromQueries(...queries: string[]) {
  return eventDefinitionForSchema('test_schema', ...queries);
}

function eventDefinitionForSchema(defaultSchema: string, ...queries: string[]) {
  const config = SqlSyncRules.fromYaml(yamlWithEventQueries(...queries), {
    defaultSchema
  }).config as PrecompiledSyncConfig;

  return serializeSyncPlan(config.plan).events![0];
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
