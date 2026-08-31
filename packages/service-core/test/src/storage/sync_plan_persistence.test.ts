import { DEFAULT_TAG, PrecompiledSyncConfig } from '@powersync/service-sync-rules';
import { describe, expect, test } from 'vitest';
import * as storage from '../../../src/storage/storage-index.js';

// With `edition: 3`, sync configs are persisted together with a compiled sync plan, and replication restores
// the config from the stored plan instead of re-parsing the YAML (see parsePersistedSyncConfigContent).
// Everything that lives outside the plan itself — like initial snapshot filters — must survive that round-trip.
const SYNC_RULES_YAML = `
initial_snapshot_filters:
  lists:
    sql: "archived = false"
  "%":
    sql: "deleted_at IS NULL"

streams:
  global:
    query: SELECT * FROM lists

config:
  edition: 3
`;

function roundTrip(mutate?: (stored: any) => void) {
  const update = storage.updateSyncRulesFromYaml(SYNC_RULES_YAML, { validate: true });
  expect(update.config.plan).not.toBeNull();

  // Simulate persistence of the serialized plan (JSONB for the Postgres storage module).
  const stored = JSON.parse(JSON.stringify(update.config.plan));
  mutate?.(stored);

  return storage.parsePersistedSyncConfigContent({
    content: update.config.yaml,
    compiledPlan: stored,
    storageVersion: update.config.parsed.config.storageVersion ?? 1,
    parseOptions: { defaultSchema: 'public' }
  });
}

describe('sync plan persistence', () => {
  test('initial snapshot filters survive the compiled plan round-trip', () => {
    const { config } = roundTrip();
    expect(config).toBeInstanceOf(PrecompiledSyncConfig);

    expect(config.getInitialSnapshotFilter(DEFAULT_TAG, 'public', 'lists')).toEqual({ sql: 'archived = false' });
    // The specific entry must keep winning over the wildcard after the round-trip (first match wins).
    expect(config.getInitialSnapshotFilter(DEFAULT_TAG, 'public', 'other_table')).toEqual({
      sql: 'deleted_at IS NULL'
    });
  });

  test('plans stored before filters were serialized are treated as having no filters', () => {
    const { config } = roundTrip((stored) => {
      delete stored.initialSnapshotFilters;
    });
    expect(config).toBeInstanceOf(PrecompiledSyncConfig);
    expect(config.getInitialSnapshotFilter(DEFAULT_TAG, 'public', 'lists')).toBeUndefined();
  });
});
