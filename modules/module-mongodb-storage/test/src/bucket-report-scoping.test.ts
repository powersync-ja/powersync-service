import { MongoSyncBucketStorageV3 } from '@module/storage/implementation/v3/MongoSyncBucketStorageV3.js';
import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { test_utils } from '@powersync/service-core-tests';
import * as bson from 'bson';
import { describe, expect, test } from 'vitest';
import { INITIALIZED_MONGO_STORAGE_FACTORY } from './util.js';

function sourceDescriptor(name: string, objectId: string): storage.SourceEntityDescriptor {
  return {
    connectionTag: storage.SourceTable.DEFAULT_TAG,
    objectId,
    schema: 'public',
    name,
    replicaIdColumns: [{ name: 'id', type: 'VARCHAR', typeId: 25 }]
  };
}

function objectIdGenerator(id: string) {
  let used = false;
  return () => {
    if (used) {
      throw new Error(`Can only generate a single id using ${id}`);
    }
    used = true;
    return new bson.ObjectId(id);
  };
}

/**
 * In V3 a replication stream can host multiple sync configs (active + stopped, until cleanup runs), all sharing
 * the per-stream bucket_state and source_records collections. The report must only include the active config's
 * bucket definitions, not stale ones from a previous (now stopped) config.
 */
describe('bucket report scoping - mongodb v3', () => {
  test('excludes buckets from stopped/old sync configs sharing the replication stream', async () => {
    await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY.factory();

    // Config 1: replicate todos. Writing a row creates a data bucket for this config.
    const first = await factory.updateSyncRules(
      updateSyncRulesFromYaml(
        `
config:
  edition: 3

streams:
  by_owner:
    query: SELECT * FROM todos WHERE owner_id = subscription.parameter('owner_id')
`,
        { storageVersion: 3 }
      )
    );
    const firstStorage = factory.getInstance(first) as MongoSyncBucketStorageV3;
    await using firstWriter = await firstStorage.createWriter(test_utils.BATCH_OPTIONS);
    const todosTable = (
      await firstWriter.resolveTables({
        connection_id: 1,
        source: sourceDescriptor('todos', 'todos-relation'),
        idGenerator: objectIdGenerator('6544e3899293153fa7b38360')
      })
    ).tables[0];
    await firstWriter.save({
      sourceTable: todosTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'todo-1', owner_id: 'user-1' },
      afterReplicaId: test_utils.rid('todo-1')
    });
    await firstWriter.markAllSnapshotDone('1/1');
    await firstWriter.commit('1/1');
    await firstWriter.flush();

    // While config 1 is active, its bucket(s) show up in the report.
    const firstReport = await firstStorage.getBucketReport();
    expect(firstReport.totals.bucketCount).toBeGreaterThan(0);

    // Config 2: a different stream over a different table. Config 1 transitions to STOP, but its bucket_state
    // and source_records rows remain in the shared collections until cleanup runs (which we deliberately skip).
    const second = await factory.updateSyncRules(
      updateSyncRulesFromYaml(
        `
config:
  edition: 3

streams:
  by_project:
    query: SELECT * FROM scenes WHERE project_id = subscription.parameter('project_id')
`,
        { storageVersion: 3 }
      )
    );
    expect(second.replicationStreamId).toBe(first.replicationStreamId);

    // Drive config 2 to snapshot-done so it becomes ACTIVE and config 1 transitions to STOP (config 1 keeps
    // serving until the new config finishes processing). Config 1's stale rows remain until cleanup, which we skip.
    const replicatingStreams = await factory.getReplicatingReplicationStreams();
    expect(replicatingStreams).toHaveLength(1);
    const secondStorage = factory.getInstance(replicatingStreams[0]) as MongoSyncBucketStorageV3;
    await using secondWriter = await secondStorage.createWriter(test_utils.BATCH_OPTIONS);
    await secondWriter.markAllSnapshotDone('2/1');
    await secondWriter.commit('2/1');
    await secondWriter.flush();

    const activeConfig = await factory.getActiveSyncConfig();
    expect(activeConfig).not.toBeNull();
    const activeStorage = activeConfig!.storage as MongoSyncBucketStorageV3;
    const secondReport = await activeStorage.getBucketReport();

    // Config 2 has no replicated data, and config 1's stale buckets must be excluded. Without scoping to the
    // active config's definition ids, config 1's bucket would leak in here.
    expect(secondReport.totals.bucketCount).toEqual(0);
    const firstBucketNames = new Set(firstReport.buckets.map((b) => b.bucket));
    expect(secondReport.buckets.some((b) => firstBucketNames.has(b.bucket))).toBe(false);
  });
});
