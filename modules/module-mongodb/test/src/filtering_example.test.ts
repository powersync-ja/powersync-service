import {
  filterMongoReplicationStream,
  MongoReplicationFilter,
  MongoReplicationFilterResult,
  MongoReplicationQueryProvider,
  parseMongoReplicationStream,
  RawMongoReplicationStreamBatch
} from '@module/index.js';
import { mongo } from '@powersync/lib-service-mongodb';
import { expect, test } from 'vitest';

/**
 * Minimal provider example: use the same immutable ID prefix for snapshots and streaming.
 * Real BSON parsing and the public filtering helper run without a database connection.
 */
test('example: replicate only documents with public IDs and preserve filtered progress', async () => {
  const filter: MongoReplicationFilter = (event) =>
    String(event.documentKey._id).startsWith('public-')
      ? MongoReplicationFilterResult.KEEP
      : MongoReplicationFilterResult.EXCLUDE;
  const provider: MongoReplicationQueryProvider = {
    getSnapshotFilter: () => ({ $expr: { $regexMatch: { input: '$_id', regex: '^public-' } } }),
    openChangeStream: ({ open }) => filterMongoReplicationStream({ batches: open({}), filter })
  };

  async function* source(): AsyncGenerator<RawMongoReplicationStreamBatch> {
    for (const ids of [['public-one', 'private-one', 'public-two'], ['private-two']]) {
      yield {
        events: ids.map((_id) =>
          Buffer.from(
            mongo.BSON.serialize({
              _id: { _data: _id },
              operationType: 'insert',
              ns: { db: 'app', coll: 'documents' },
              documentKey: { _id },
              fullDocument: { _id }
            })
          )
        ),
        resumeToken: { _data: ids.at(-1) }
      };
    }
  }

  const stream = provider.openChangeStream({
    open: () => parseMongoReplicationStream({ batches: source(), defaultSchema: 'app', multipleDatabases: false })
  });
  const retainedIds = [];
  const progress = [];
  for await (const batch of stream) {
    for (const { event } of batch.events) {
      if ('documentKey' in event) {
        retainedIds.push(event.documentKey._id);
      }
    }
    // Read progress only after exhausting the events, including a fully filtered batch.
    progress.push({ token: batch.getResumeToken(), filteredCount: batch.metadata.filteredCount });
  }

  expect(retainedIds).toEqual(['public-one', 'public-two']);
  expect(progress).toEqual([
    { token: { _data: 'public-two' }, filteredCount: 1 },
    { token: { _data: 'private-two' }, filteredCount: 1 }
  ]);
});
