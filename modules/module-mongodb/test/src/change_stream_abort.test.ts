import { DEFAULT_MONGO_REPLICATION_QUERY_PROVIDER } from '@module/replication/MongoReplicationQueryProvider.js';
import { createMongoReplicationStreamBatch } from '@module/replication/MongoReplicationStream.js';
import type { MongoReplicationStreamBatch } from '@module/replication/MongoReplicationStreamTypes.js';
import { ReplicationAbortedError } from '@powersync/lib-services-framework';
import { describe, expect, test } from 'vitest';
import { openChangeStreamTestContext } from './change_stream_test_setup.js';
import { DATABASE_TYPE, DatabaseType } from './DatabaseType.js';
import { describeWithStorage } from './util.js';

type AbortPoint = 'mid-batch' | 'after-last-event';

/**
 * Abort replication from inside event processing, using real source changes and bucket storage.
 * The abort fires while the consumer holds an event, so it lands between the parser's own abort checks.
 */
describe.skipIf(DATABASE_TYPE == DatabaseType.DOCUMENTDB)('MongoDB change stream abort during a batch', () => {
  describeWithStorage({ timeout: 30_000 }, ({ factory, storageVersion }) => {
    test.each<AbortPoint>(['mid-batch', 'after-last-event'])('stops replication when aborted %s', async (point) => {
      {
        await using context = await openChangeStreamTestContext(factory, { storageVersion });
        await context.updateSyncRules(/* yaml */ `
          # Sync config fixture.
          config:
            edition: 3
          streams:
            global:
              query: SELECT _id AS id FROM documents
        `);
        await context.db.createCollection('documents');
        await context.replicateSnapshot();
        await context.getCheckpoint();
        await context.stop();
        await context.db
          .collection<{ _id: number }>('documents')
          .insertMany(Array.from({ length: 20 }, (_, i) => ({ _id: i + 1 })));
      }

      const abortCause = new Error('test abort');
      let abort: (cause: Error) => void = () => {};
      let triggered = false;
      let resumeBeforeAbortedBatch: string | null | undefined;
      let readResumeLsn: () => Promise<string | null | undefined>;
      let sawDocument = false;
      const provider = {
        ...DEFAULT_MONGO_REPLICATION_QUERY_PROVIDER,
        openChangeStream: ({ open }: Parameters<typeof DEFAULT_MONGO_REPLICATION_QUERY_PROVIDER.openChangeStream>[0]) =>
          (async function* (): AsyncGenerator<MongoReplicationStreamBatch> {
            for await (const batch of open({})) {
              const resumeBeforeBatch = await readResumeLsn();
              yield createMongoReplicationStreamBatch({
                events: (function* () {
                  for (const change of batch.events) {
                    if ('ns' in change.event && change.event.ns.coll == 'documents') {
                      sawDocument = true;
                    }
                    yield change;
                    // Runs when the consumer asks for the next event, after it has processed this one.
                    const atPoint = point == 'mid-batch' ? change.hasBufferedChanges : !change.hasBufferedChanges;
                    if (!triggered && sawDocument && atPoint) {
                      triggered = true;
                      resumeBeforeAbortedBatch = resumeBeforeBatch;
                      abort(abortCause);
                    }
                  }
                })(),
                resolveResumeToken: () => batch.getResumeToken(),
                resolveMetadata: () => batch.metadata
              });
            }
          })()
      };

      {
        await using context = await openChangeStreamTestContext(factory, {
          storageVersion,
          doNotClear: true,
          streamOptions: { createReplicationQueryProvider: () => provider }
        });
        readResumeLsn = async () => (await context.storage!.getStatus()).resumeLsn;
        abort = (cause) => context.abort(cause);
        await context.loadActiveSyncRules();
        const result = await context.startStreaming();

        expect(triggered).toBe(true);
        expect(result.status).toBe('rejected');
        if (result.status != 'rejected') {
          throw new Error('Expected replication to abort');
        }
        if (point == 'after-last-event') {
          // Every event of the batch was processed, so the consumer's own abort check ends the stream
          // before it persists the batch's resume token.
          expect(result.reason).toBeInstanceOf(ReplicationAbortedError);
        }
        const cause = result.reason instanceof ReplicationAbortedError ? result.reason.cause : result.reason;
        expect(cause).toBe(abortCause);
        expect(resumeBeforeAbortedBatch).toBeTruthy();
        expect((await context.storage!.getStatus()).resumeLsn).toBe(resumeBeforeAbortedBatch);
      }

      // Reopen the real storage after the interrupted batch and verify all source rows are recovered.
      await using resumed = await openChangeStreamTestContext(factory, { storageVersion, doNotClear: true });
      await resumed.loadActiveSyncRules();
      resumed.startStreaming();
      const rows = (await resumed.getBucketData('global|0[]')).filter((op) => op.op == 'PUT');
      expect([...new Set(rows.map((op) => op.object_id))].sort()).toEqual(
        Array.from({ length: 20 }, (_, index) => String(index + 1)).sort()
      );
    });
  });
});
