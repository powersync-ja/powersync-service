import {
  DEFAULT_HYDRATION_STATE,
  DEFAULT_TAG,
  nodeSqlite,
  PrecompiledSyncConfig,
  serializeSyncPlan,
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
    const originalEvent = compiled.plan.events![0];

    expect(compiled.plan.version).toBeLessThanOrEqual(2);
    expect(compiled.plan.events).toHaveLength(1);
    expect(originalEvent.name).toBe('write_checkpoints');
    expect(compiled.eventDescriptors).toEqual({ write_checkpoints: [EVENT_QUERY] });

    const restored = restore(compiled);
    expect(restored.config).toBeInstanceOf(PrecompiledSyncConfig);
    expect(restored.config).not.toHaveProperty('eventDescriptors');
    expect(restored.config.eventDefinitions).toHaveLength(1);
    expect(restored.config.eventDefinitions[0].name).toBe('write_checkpoints');

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
    // mirror is normalized into the compiled representation by the new loading boundary, retaining the legacy behavior
    // of ignoring payload filters until the sync config is redeployed.
    const { events: _ignored, ...planWithoutCompiledEvents } = compiled.plan;
    const legacyView = restore({ ...compiled, plan: planWithoutCompiledEvents });
    expect(legacyView.config).not.toHaveProperty('eventDescriptors');
    expect(legacyView.config.eventDefinitions).toHaveLength(1);
    expect(legacyView.config.eventDefinitions[0].name).toBe('write_checkpoints');
    expect(legacyView.errors).toHaveLength(1);
    expect(legacyView.errors[0]).toMatchObject({ type: 'warning' });

    const legacyEvent = serializeSyncPlan((legacyView.config as PrecompiledSyncConfig).plan).events![0];
    expect(legacyEvent.sourceQueries[0].variants[0].filters).toEqual([]);
    expect(originalEvent.sourceQueries[0].variants[0].filters).not.toEqual([]);

    const legacyHydrated = legacyView.config.hydrate({
      hydrationState: DEFAULT_HYDRATION_STATE,
      sqlite: nodeSqlite(sqlite)
    });
    expect(
      legacyHydrated.eventDescriptors[0].evaluateRowWithErrors({
        sourceTable: { connectionTag: DEFAULT_TAG, schema: 'test_schema', name: 'checkpoints' },
        record: { user_id: 'user-1', checkpoint: 4n, active: 0 }
      })
    ).toEqual({ result: { data: { user_id: 'user-1', checkpoint: 4n } }, errors: [] });
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
    expect(restored.errors).toHaveLength(1);
    expect(restored.errors[0]).toMatchObject({ type: 'warning' });
    expect(
      serializeSyncPlan((restored.config as PrecompiledSyncConfig).plan).events![0].sourceQueries[0].variants[0].filters
    ).toEqual([]);
  });

  /**
   * Event SQL accepted by the legacy evaluator must remain loadable after an upgrade, even when its ignored filter uses
   * expressions that the compiled event evaluator intentionally rejects for new deployments.
   */
  test.each([
    ['request parameters', 'SELECT user_id FROM checkpoints WHERE user_id = request.user_id()'],
    ['unsupported expressions', 'SELECT user_id FROM checkpoints WHERE active IN (1, 2)']
  ])('loads legacy event descriptors without validating ignored %s', (_description, eventQuery) => {
    const parsed = SqlSyncRules.fromYaml(yamlWithoutEvents, { defaultSchema: 'test_schema' });
    const update = updateSyncRulesFromConfig(parsed);
    const legacy: SerializedSyncPlan = {
      ...update.config.plan!,
      eventDescriptors: {
        write_checkpoints: [eventQuery]
      }
    };

    const restored = restore(legacy);
    expect(restored.errors).toHaveLength(1);
    expect(restored.errors[0]).toMatchObject({ type: 'warning' });

    const event = restored.config.hydrate({
      hydrationState: DEFAULT_HYDRATION_STATE,
      sqlite: nodeSqlite(sqlite)
    }).eventDescriptors[0];
    expect(
      event.evaluateRowWithErrors({
        sourceTable: { connectionTag: DEFAULT_TAG, schema: 'test_schema', name: 'checkpoints' },
        record: { user_id: 'user-1' }
      })
    ).toEqual({ result: { data: { user_id: 'user-1' } }, errors: [] });
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
