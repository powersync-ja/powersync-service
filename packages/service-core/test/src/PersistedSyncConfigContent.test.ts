import {
  DEFAULT_HYDRATION_STATE,
  DEFAULT_TAG,
  nodeSqlite,
  PrecompiledSyncConfig,
  SqlSyncRules
} from '@powersync/service-sync-rules';
import * as sqlite from 'node:sqlite';
import { describe, expect, test } from 'vitest';
import {
  parsePersistedSyncConfigContent,
  SerializedSyncPlan,
  updateSyncRulesFromConfig
} from '../../src/storage/storage-index.js';

const EVENT_QUERY = 'SELECT user_id, checkpoint FROM checkpoints WHERE active = true';
const yamlWithoutEvents = `
config:
  edition: 3

streams:
  checkpoints:
    query: SELECT * FROM checkpoints
`;
const yamlWithEvents = `${yamlWithoutEvents}
event_definitions:
  write_checkpoints:
    payloads:
      - ${EVENT_QUERY}
`;

describe('persisted compiled replication events', () => {
  test('dual-writes additive compiled events and restores the compiled evaluator', () => {
    const parsed = SqlSyncRules.fromYaml(yamlWithEvents, { defaultSchema: 'test_schema' });
    const update = updateSyncRulesFromConfig(parsed);
    const compiled = update.config.plan!;

    expect(compiled.plan.version).toBeLessThanOrEqual(2);
    expect(compiled.plan.events).toHaveLength(1);
    expect(compiled.eventDescriptors).toEqual({ write_checkpoints: [EVENT_QUERY] });

    const restored = restore(compiled);
    expect(restored.config).toBeInstanceOf(PrecompiledSyncConfig);
    expect(restored.config).not.toHaveProperty('eventDescriptors');
    expect(restored.config.eventDefinitions).toHaveLength(1);

    const hydrated = restored.config.hydrate({
      hydrationState: DEFAULT_HYDRATION_STATE,
      sqlite: nodeSqlite(sqlite)
    });
    expect(
      hydrated.eventDescriptors[0].evaluateRowWithErrors({
        sourceTable: { connectionTag: DEFAULT_TAG, schema: 'test_schema', name: 'checkpoints' },
        record: { user_id: 'user-1', checkpoint: 4n, active: 1 }
      })
    ).toEqual({ result: { data: { user_id: 'user-1', checkpoint: 4n } }, errors: [] });

    // This represents what an older compiler sees after ignoring the additive plan.events field. The raw descriptor
    // mirror is normalized into the compiled representation by the new loading boundary.
    const { events: _ignored, ...planWithoutCompiledEvents } = compiled.plan;
    const legacyView = restore({ ...compiled, plan: planWithoutCompiledEvents });
    expect(legacyView.config).not.toHaveProperty('eventDescriptors');
    expect(legacyView.config.eventDefinitions).toHaveLength(1);
    expect((legacyView.config as PrecompiledSyncConfig).plan.events).toHaveLength(1);
  });

  test('restores raw event descriptors attached to version 1 and 2 plans', () => {
    const parsed = SqlSyncRules.fromYaml(yamlWithoutEvents, { defaultSchema: 'test_schema' });
    const update = updateSyncRulesFromConfig(parsed);
    const legacy: SerializedSyncPlan = {
      ...update.config.plan!,
      eventDescriptors: { write_checkpoints: [EVENT_QUERY] }
    };

    expect(legacy.plan.version).toBeLessThanOrEqual(2);
    const restored = restore(legacy);
    expect(restored.config).not.toHaveProperty('eventDescriptors');
    expect(restored.config.eventDefinitions).toHaveLength(1);
    expect((restored.config as PrecompiledSyncConfig).plan.events).toMatchObject([
      { name: 'write_checkpoints', sourceQueries: [{ sql: EVENT_QUERY }] }
    ]);
  });
});

function restore(compiledPlan: SerializedSyncPlan) {
  return parsePersistedSyncConfigContent({
    content: yamlWithEvents,
    compiledPlan,
    storageVersion: 1,
    parseOptions: { defaultSchema: 'test_schema' }
  });
}
