import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { bucketRequest, test_utils } from '@powersync/service-core-tests';
import { describe, expect, test } from 'vitest';
import { ChangeStreamTestContext } from './change_stream_utils.js';
import { INITIALIZED_MONGO_STORAGE_FACTORY } from './util.js';

const VALID_CONFIG = /* yaml */ `
  # Baseline valid deployment.
  config:
    edition: 3
  streams:
    todos:
      auto_subscribe: true
      query: SELECT _id AS id, description FROM todos
`;

const INVALID_CONFIG = /* yaml */ `
  # Valid stream plus a fatal SQL error.
  config:
    edition: 3
  streams:
    todos:
      auto_subscribe: true
      query: SELECT _id AS id, description FROM todos
    broken:
      query: SELECT FROM
`;

const CHANGED_CONFIG = /* yaml */ `
  # Valid deployment adding a stream.
  config:
    edition: 3
  streams:
    todos:
      auto_subscribe: true
      query: SELECT _id AS id, description FROM todos
    projects:
      auto_subscribe: true
      query: SELECT _id AS id, name FROM projects
`;

describe.each([3, 4])('MongoDB source invalid config, storage v%i', (storageVersion) => {
  /**
   * Activate and stream A, then append invalid B. Source writes must still reach clients
   * through A. Reopen the service and deploy valid C, using either A's content or a changed
   * config; C must activate and stream existing and new data without replicating B.
   */
  test.each([VALID_CONFIG, CHANGED_CONFIG])(
    'keeps streaming past an invalid deployment and serves a subsequent valid deployment %#',
    async (replacementConfig) => {
      let streamId: number;
      {
        await using context = await ChangeStreamTestContext.open(INITIALIZED_MONGO_STORAGE_FACTORY.factory, {
          storageVersion
        });
        await context.db.collection('todos').insertOne({ _id: 'before', description: 'initial' } as any);
        await context.db.collection('projects').insertOne({ _id: 'project', name: 'new stream data' } as any);
        const first = await context.updateSyncRules(VALID_CONFIG);
        streamId = first.replicationStreamId;
        await context.replicateSnapshot();
        context.startStreaming();
        await context.getCheckpoint();

        const before = (await context.factory.getReplicatingReplicationStreams())[0];
        const invalid = await context.factory.updateSyncRules(
          updateSyncRulesFromYaml(INVALID_CONFIG, { storageVersion, validate: false })
        );
        expect(invalid.replicationStreamId).toBe(streamId);
        expect((await context.factory.getReplicatingReplicationStreams())[0].replicationJobId).toBe(
          before.replicationJobId
        );
        await context.db.collection('todos').insertOne({ _id: 'after', description: 'still streaming' } as any);
        expect(await context.getBucketData('todos|0[]')).toMatchObject([
          test_utils.putOp('todos', { id: 'before', description: 'initial' }),
          test_utils.putOp('todos', { id: 'after', description: 'still streaming' })
        ]);
        expect((await context.factory.getActiveSyncConfig())?.content.sync_rules_content).toBe(VALID_CONFIG);
        expect(await context.factory.getProcessingSyncConfig()).toBeNull();
      }

      await using context = await ChangeStreamTestContext.open(INITIALIZED_MONGO_STORAGE_FACTORY.factory, {
        storageVersion,
        doNotClear: true
      });
      const replacement = await context.updateSyncRules(replacementConfig);
      expect(replacement.replicationStreamId).toBe(streamId);
      const replicating = (await context.factory.getReplicatingReplicationStreams())[0];
      expect(replicating.syncConfigContent.map((config) => config.sync_rules_content)).not.toContain(INVALID_CONFIG);
      await context.replicateSnapshot();
      context.startStreaming();
      await context.getCheckpoint();
      expect((await context.factory.getActiveSyncConfig())?.content.sync_rules_content).toBe(replacementConfig);
      await context.loadActiveSyncRules();
      await context.db
        .collection('todos')
        .insertOne({ _id: 'replacement', description: 'replacement streaming' } as any);
      expect(await context.getBucketData('todos|0[]')).toMatchObject([
        test_utils.putOp('todos', { id: 'before', description: 'initial' }),
        test_utils.putOp('todos', { id: 'after', description: 'still streaming' }),
        test_utils.putOp('todos', { id: 'replacement', description: 'replacement streaming' })
      ]);
      if (replacementConfig == CHANGED_CONFIG) {
        expect(await context.getBucketData('projects|0[]')).toMatchObject([
          test_utils.putOp('projects', { id: 'project', name: 'new stream data' })
        ]);
      }
    },
    30_000
  );
});

describe.each([3, 4])('corrected incremental config, storage v%i', (storageVersion) => {
  /**
   * Activate A with todos and projects. Deploy invalid B that changes projects and adds
   * a malformed lists query, then correct that query in C. C must reuse A's todos bucket
   * and operations, snapshot only changed projects and added lists, and serve exactly
   * C's expected data. B must never become active or contribute client-visible buckets.
   */
  test('reprocesses only changed and added definitions after an invalid deployment', async () => {
    const corrected = /* yaml */ `
      # Corrected deployment with one unchanged, one changed, and one added stream.
      config:
        edition: 3
      streams:
        todos:
          auto_subscribe: true
          query: SELECT _id AS id, description FROM todos
        projects:
          auto_subscribe: true
          query: SELECT _id AS id, name AS title FROM projects
        lists:
          auto_subscribe: true
          query: SELECT _id AS id, name FROM lists
    `;
    const invalid = corrected.replace('SELECT _id AS id, name FROM lists', 'SELECT FROM');
    let activeContent: storage.PersistedSyncConfigContent;
    let originalTodos: Awaited<ReturnType<ChangeStreamTestContext['getBucketData']>>;
    {
      await using context = await ChangeStreamTestContext.open(INITIALIZED_MONGO_STORAGE_FACTORY.factory, {
        storageVersion
      });
      await context.db.collection('todos').insertOne({ _id: 'todo', description: 'unchanged' } as any);
      await context.db.collection('projects').insertOne({ _id: 'project', name: 'renamed field' } as any);
      await context.db.collection('lists').insertOne({ _id: 'list', name: 'new stream' } as any);
      await context.updateSyncRules(CHANGED_CONFIG);
      await context.replicateSnapshot();
      context.startStreaming();
      await context.getCheckpoint();
      await context.loadActiveSyncRules();
      activeContent = (await context.factory.getActiveSyncConfig())!.content;
      originalTodos = await context.getBucketData('todos|0[]');
      expect(originalTodos).toMatchObject([test_utils.putOp('todos', { id: 'todo', description: 'unchanged' })]);
      expect(await context.getBucketData('projects|0[]')).toMatchObject([
        test_utils.putOp('projects', { id: 'project', name: 'renamed field' })
      ]);
      await context.factory.updateSyncRules(updateSyncRulesFromYaml(invalid, { storageVersion, validate: false }));
      expect((await context.factory.getSyncConfigStatus()).next?.content.syncConfigState).toBe(
        storage.SyncRuleState.INVALID
      );
      expect((await context.factory.getActiveSyncConfig())!.content.syncConfigId).toBe(activeContent.syncConfigId);
    }

    const snapshots: string[] = [];
    await using context = await ChangeStreamTestContext.open(INITIALIZED_MONGO_STORAGE_FACTORY.factory, {
      storageVersion,
      doNotClear: true,
      streamOptions: {
        snapshotHooks: {
          beforeSnapshotStarted: async (table) => {
            snapshots.push(table.name);
          }
        }
      }
    });
    const replacement = await context.updateSyncRules(corrected);
    expect(replacement.replicationStreamId).toBe(activeContent.replicationStreamId);
    const deploying = (await context.factory.getProcessingSyncConfig())!.content;
    expect(bucketRequest(deploying, 'todos|0[]').bucket).toBe(bucketRequest(activeContent, 'todos|0[]').bucket);
    expect(bucketRequest(deploying, 'projects|0[]').bucket).not.toBe(
      bucketRequest(activeContent, 'projects|0[]').bucket
    );
    await context.replicateSnapshot();
    context.startStreaming();
    await context.getCheckpoint();
    await context.loadActiveSyncRules();
    expect(snapshots.sort()).toEqual(['lists', 'projects']);
    expect((await context.factory.getActiveSyncConfig())!.content.sync_rules_content).toBe(corrected);
    expect((await context.factory.getSyncConfigStatus()).next).toBeNull();
    expect(await context.getBucketData('todos|0[]')).toEqual(originalTodos);
    expect(await context.getBucketData('projects|0[]')).toMatchObject([
      test_utils.putOp('projects', { id: 'project', title: 'renamed field' })
    ]);
    expect(await context.getBucketData('lists|0[]')).toMatchObject([
      test_utils.putOp('lists', { id: 'list', name: 'new stream' })
    ]);
    const sources = (await context.factory.getActiveSyncConfig())!.content.parsed({
      defaultSchema: context.db.databaseName
    }).syncConfigs[0].config.bucketDataSources;
    expect(sources.map((source) => source.uniqueName).sort()).toEqual(['lists|0', 'projects|0', 'todos|0']);
  }, 30_000);
});
