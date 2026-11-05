import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import * as timers from 'node:timers/promises';
import { expect, test } from 'vitest';
import { getTestStorage } from '../test-utils/leased-storage.js';
import * as test_utils from '../test-utils/test-utils-index.js';

/**
 * Snapshot progress persisted by `updateTableProgress()`.
 *
 * The rate calculation itself is covered by the `mergeTableProgress` unit tests in service-core. These tests check
 * that each storage persists and hydrates the merged status.
 *
 * @example
 * ```TypeScript
 *
 * describe('snapshot progress - mongodb', function () {
 *  registerSnapshotProgressTests(MONGO_STORAGE_FACTORY);
 * });
 *
 * ```
 */
export function registerSnapshotProgressTests(config: storage.TestStorageConfig) {
  const generateStorageFactory = config.factory;
  const storageVersion = config.storageVersion;

  async function createWriter(factory: Awaited<ReturnType<typeof generateStorageFactory>>) {
    const { stream } = await test_utils.deploySyncRules(
      factory,
      updateSyncRulesFromYaml(
        `
config:
  edition: 3

streams:
  test:
    auto_subscribe: true
    query: SELECT id, description FROM test
`,
        { storageVersion }
      )
    );
    const bucketStorage = await getTestStorage(factory, stream);
    return bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
  }

  async function resolveTable(writer: storage.BucketStorageBatch) {
    const table = await test_utils.resolveTestTable(writer, 'test', ['id'], config);
    expect(table.snapshotComplete).toBe(false);
    return table;
  }

  /**
   * Reads the persisted status back from storage.
   *
   * lastKey is normalized to a Buffer, since storages may hydrate it as a Buffer or a plain Uint8Array.
   */
  async function persistedStatus(writer: storage.BucketStorageBatch, table: storage.SourceTable) {
    const persisted = await writer.getSourceTableStatus(table);
    return normalize(persisted?.snapshotStatus);
  }

  function normalize(status: storage.TableSnapshotStatus | undefined) {
    if (status == null) {
      return status;
    }
    return { ...status, lastKey: status.lastKey == null ? null : Buffer.from(status.lastKey) };
  }

  test('persists observation time and replication rate', async () => {
    await using factory = await generateStorageFactory();
    await using writer = await createWriter(factory);
    const table = await resolveTable(writer);

    // Estimate-only: no observation yet
    const estimated = await writer.updateTableProgress(table, { totalEstimatedCount: 1000 });
    expect(await persistedStatus(writer, estimated)).toEqual({
      totalEstimatedCount: 1000,
      replicatedCount: 0,
      lastKey: null,
      updatedAt: null,
      replicationRate: null
    });

    // First chunk: sets the observation time, no rate yet
    const firstChunk = await writer.updateTableProgress(estimated, {
      replicatedCount: 100,
      lastKey: new Uint8Array([1, 2, 3])
    });
    expect(firstChunk.snapshotStatus?.updatedAt).toBeInstanceOf(Date);
    expect(firstChunk.snapshotStatus?.replicationRate).toBeNull();
    expect(await persistedStatus(writer, firstChunk)).toEqual(normalize(firstChunk.snapshotStatus));

    // Second chunk: the rate becomes measurable. Simulate a 10ms delay to ensure the rate is measurable.
    await timers.setTimeout(10);
    const secondChunk = await writer.updateTableProgress(firstChunk, { replicatedCount: 300 });
    expect(secondChunk.snapshotStatus?.replicationRate).toBeGreaterThan(0);
    expect(await persistedStatus(writer, secondChunk)).toEqual(normalize(secondChunk.snapshotStatus));
  });

  test('a missing estimate stays not estimated, and unknown stays unknown', async () => {
    await using factory = await generateStorageFactory();
    await using writer = await createWriter(factory);
    const table = await resolveTable(writer);

    const notEstimated = await writer.updateTableProgress(table, { replicatedCount: 10 });
    expect((await persistedStatus(writer, notEstimated))?.totalEstimatedCount).toBeNull();

    const unknown = await writer.updateTableProgress(notEstimated, { totalEstimatedCount: -1 });
    expect((await persistedStatus(writer, unknown))?.totalEstimatedCount).toEqual(-1);
  });

  test('persists counts beyond 32-bit integers', async () => {
    await using factory = await generateStorageFactory();
    await using writer = await createWriter(factory);
    const table = await resolveTable(writer);

    const large = await writer.updateTableProgress(table, {
      totalEstimatedCount: 5_000_000_000,
      replicatedCount: 3_000_000_000
    });
    expect(await persistedStatus(writer, large)).toMatchObject({
      totalEstimatedCount: 5_000_000_000,
      replicatedCount: 3_000_000_000
    });
  });
}
