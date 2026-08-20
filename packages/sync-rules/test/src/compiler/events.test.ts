import * as sqlite from 'node:sqlite';
import { describe, expect, test } from 'vitest';
import {
  DEFAULT_HYDRATION_STATE,
  deserializeSyncPlan,
  nodeSqlite,
  PrecompiledSyncConfig,
  serializedEventDefinitionId,
  serializedEventDefinitionIdentity,
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
    expect(compiled.eventDefinitions[0].id).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/
    );
    expect(serialized.events![0].id).toBe(compiled.eventDefinitions[0].id);
    const deserialized = deserializeSyncPlan(JSON.parse(JSON.stringify(serialized)));
    expect(deserialized.events).toMatchObject(compiled.plan.events);
    expect(deserialized.events[0].id).toBe(compiled.eventDefinitions[0].id);

    const hydrated = compiled.hydrate({ hydrationState: DEFAULT_HYDRATION_STATE, sqlite: nodeSqlite(sqlite) });
    const event = hydrated.eventDescriptors[0];
    expect(event.id).toBe(compiled.eventDefinitions[0].id);
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

  test('derives a canonical id independent of formatting, filter order and payload query order', () => {
    const first = eventDefinitionFromQueries(
      'SELECT user_id, checkpoint FROM checkpoints WHERE active = true AND checkpoint > 0',
      'SELECT user_id, checkpoint FROM archived_checkpoints'
    );
    const reordered = eventDefinitionFromQueries(
      ' select "user_id", "checkpoint" from "archived_checkpoints" ',
      'select "user_id", "checkpoint" from "checkpoints" c where c."checkpoint" > 0 and c."active" = true'
    );
    const changed = eventDefinitionFromQueries(
      'SELECT user_id, checkpoint FROM checkpoints WHERE active = true AND checkpoint > 1',
      'SELECT user_id, checkpoint FROM archived_checkpoints'
    );

    expect(first.id).toBe(serializedEventDefinitionId(first.event));
    expect(reordered.id).toBe(first.id);
    expect(changed.id).not.toBe(first.id);
    expect(serializedEventDefinitionIdentity(reordered.event)).toBe(serializedEventDefinitionIdentity(first.event));
  });

  test('excludes raw SQL, compiler hashes and variant order from the id', () => {
    const first = eventDefinitionFromQueries('SELECT user_id, checkpoint FROM checkpoints WHERE active = true');
    const second = eventDefinitionFromQueries('SELECT user_id, checkpoint FROM checkpoints WHERE checkpoint > 0');
    const definition = structuredClone(first.event);
    definition.sourceQueries[0].variants.push(structuredClone(second.event.sourceQueries[0].variants[0]));
    const modified = structuredClone(definition);
    modified.sourceQueries[0].sql = 'raw sql is compatibility metadata';
    modified.sourceQueries[0].variants.reverse();
    for (const variant of modified.sourceQueries[0].variants) {
      variant.hash += 1;
    }

    expect(modified.sourceQueries[0].variants).toHaveLength(2);
    expect(serializedEventDefinitionIdentity(modified)).toBe(serializedEventDefinitionIdentity(definition));
    expect(serializedEventDefinitionId(modified)).toBe(serializedEventDefinitionId(definition));
  });

  test('derives the id from the plan without loading context', () => {
    const query = 'SELECT user_id, checkpoint FROM checkpoints WHERE active = true';

    expect(eventDefinitionForSchema('first_schema', query).id).toBe(
      eventDefinitionForSchema('second_schema', query).id
    );
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
  const plan = serializeSyncPlan(config.plan);

  return { id: config.eventDefinitions[0].id, event: plan.events![0] };
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
