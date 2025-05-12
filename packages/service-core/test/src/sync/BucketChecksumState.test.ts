import {
  BucketChecksum,
  BucketChecksumState,
  BucketChecksumStateStorage,
  CHECKPOINT_INVALIDATE_ALL,
  ChecksumMap,
  InternalOpId,
  SyncContext,
  WatchFilterEvent
} from '@/index.js';
import { JSONBig } from '@powersync/service-jsonbig';
import { RequestParameters, SqliteJsonRow, ParameterLookup, SqlSyncRules } from '@powersync/service-sync-rules';
import { describe, expect, test } from 'vitest';

describe('BucketChecksumState', () => {
  // Single global[] bucket.
  // We don't care about data in these tests
  const SYNC_RULES_GLOBAL = SqlSyncRules.fromYaml(
    `
bucket_definitions:
  global:
    data: []
    `,
    { defaultSchema: 'public' }
  );

  // global[1] and global[2]
  const SYNC_RULES_GLOBAL_TWO = SqlSyncRules.fromYaml(
    `
bucket_definitions:
  global:
    parameters:
      - select 1 as id
      - select 2 as id
    data: []
    `,
    { defaultSchema: 'public' }
  );

  // by_project[n]
  const SYNC_RULES_DYNAMIC = SqlSyncRules.fromYaml(
    `
bucket_definitions:
  by_project:
    parameters: select id from projects where user_id = request.user_id()
    data: []
    `,
    { defaultSchema: 'public' }
  );

  const syncContext = new SyncContext({
    maxBuckets: 100,
    maxParameterQueryResults: 100,
    maxDataFetchConcurrency: 10
  });

  test('global bucket with update', async () => {
    const storage = new MockBucketChecksumStateStorage();
    // Set intial state
    storage.updateTestChecksum({ bucket: 'global[]', checksum: 1, count: 1 });

    const state = new BucketChecksumState({
      syncContext,
      syncParams: new RequestParameters({ sub: '' }, {}),
      syncRules: SYNC_RULES_GLOBAL,
      bucketStorage: storage
    });

    const line = (await state.buildNextCheckpointLine({
      base: { checkpoint: 1n, lsn: '1' },
      writeCheckpoint: null,
      update: CHECKPOINT_INVALIDATE_ALL
    }))!;
    expect(line.checkpointLine).toEqual({
      checkpoint: {
        buckets: [{ bucket: 'global[]', checksum: 1, count: 1, priority: 3 }],
        last_op_id: '1',
        write_checkpoint: undefined
      }
    });
    expect(line.bucketsToFetch).toEqual([
      {
        bucket: 'global[]',
        priority: 3
      }
    ]);
    // This is the bucket data to be fetched
    expect(line.getFilteredBucketPositions()).toEqual(new Map([['global[]', 0n]]));

    // This similuates the bucket data being sent
    line.advance();
    line.updateBucketPosition({ bucket: 'global[]', nextAfter: 1n, hasMore: false });

    // Update bucket storage state
    storage.updateTestChecksum({ bucket: 'global[]', checksum: 2, count: 2 });

    // Now we get a new line
    const line2 = (await state.buildNextCheckpointLine({
      base: { checkpoint: 2n, lsn: '2' },
      writeCheckpoint: null,
      update: {
        updatedDataBuckets: new Set(['global[]']),
        invalidateDataBuckets: false,
        updatedParameterLookups: new Set(),
        invalidateParameterBuckets: false
      }
    }))!;
    expect(line2.checkpointLine).toEqual({
      checkpoint_diff: {
        removed_buckets: [],
        updated_buckets: [{ bucket: 'global[]', checksum: 2, count: 2, priority: 3 }],
        last_op_id: '2',
        write_checkpoint: undefined
      }
    });
    expect(line2.getFilteredBucketPositions()).toEqual(new Map([['global[]', 1n]]));
  });

  test('global bucket with initial state', async () => {
    // This tests the client sending an initial state
    // This does not affect the checkpoint, but does affect the data to be fetched
    /// (getFilteredBucketStates)
    const storage = new MockBucketChecksumStateStorage();
    // Set intial state
    storage.updateTestChecksum({ bucket: 'global[]', checksum: 1, count: 1 });

    const state = new BucketChecksumState({
      syncContext,
      // Client sets the initial state here
      initialBucketPositions: [{ name: 'global[]', after: 1n }],
      syncParams: new RequestParameters({ sub: '' }, {}),
      syncRules: SYNC_RULES_GLOBAL,
      bucketStorage: storage
    });

    const line = (await state.buildNextCheckpointLine({
      base: { checkpoint: 1n, lsn: '1' },
      writeCheckpoint: null,
      update: CHECKPOINT_INVALIDATE_ALL
    }))!;
    expect(line.checkpointLine).toEqual({
      checkpoint: {
        buckets: [{ bucket: 'global[]', checksum: 1, count: 1, priority: 3 }],
        last_op_id: '1',
        write_checkpoint: undefined
      }
    });
    expect(line.bucketsToFetch).toEqual([
      {
        bucket: 'global[]',
        priority: 3
      }
    ]);
    // This is the main difference between this and the previous test
    expect(line.getFilteredBucketPositions()).toEqual(new Map([['global[]', 1n]]));
  });

  test('multiple static buckets', async () => {
    const storage = new MockBucketChecksumStateStorage();
    // Set intial state
    storage.updateTestChecksum({ bucket: 'global[1]', checksum: 1, count: 1 });
    storage.updateTestChecksum({ bucket: 'global[2]', checksum: 1, count: 1 });

    const state = new BucketChecksumState({
      syncContext,
      syncParams: new RequestParameters({ sub: '' }, {}),
      syncRules: SYNC_RULES_GLOBAL_TWO,
      bucketStorage: storage
    });

    const line = (await state.buildNextCheckpointLine({
      base: { checkpoint: 1n, lsn: '1' },
      writeCheckpoint: null,
      update: CHECKPOINT_INVALIDATE_ALL
    }))!;
    expect(line.checkpointLine).toEqual({
      checkpoint: {
        buckets: [
          { bucket: 'global[1]', checksum: 1, count: 1, priority: 3 },
          { bucket: 'global[2]', checksum: 1, count: 1, priority: 3 }
        ],
        last_op_id: '1',
        write_checkpoint: undefined
      }
    });
    expect(line.bucketsToFetch).toEqual([
      {
        bucket: 'global[1]',
        priority: 3
      },
      {
        bucket: 'global[2]',
        priority: 3
      }
    ]);
    line.advance();

    storage.updateTestChecksum({ bucket: 'global[1]', checksum: 2, count: 2 });
    storage.updateTestChecksum({ bucket: 'global[2]', checksum: 2, count: 2 });

    const line2 = (await state.buildNextCheckpointLine({
      base: { checkpoint: 2n, lsn: '2' },
      writeCheckpoint: null,
      update: {
        ...CHECKPOINT_INVALIDATE_ALL,
        updatedDataBuckets: new Set(['global[1]', 'global[2]']),
        invalidateDataBuckets: false
      }
    }))!;
    expect(line2.checkpointLine).toEqual({
      checkpoint_diff: {
        removed_buckets: [],
        updated_buckets: [
          { bucket: 'global[1]', checksum: 2, count: 2, priority: 3 },
          { bucket: 'global[2]', checksum: 2, count: 2, priority: 3 }
        ],
        last_op_id: '2',
        write_checkpoint: undefined
      }
    });
  });

  test('removing a static bucket', async () => {
    // This tests the client sending an initial state, with a bucket that we don't have.
    // This makes effectively no difference to the output. By not including the bucket
    // in the output, the client will remove the bucket.
    const storage = new MockBucketChecksumStateStorage();

    const state = new BucketChecksumState({
      syncContext,
      // Client sets the initial state here
      initialBucketPositions: [{ name: 'something_here[]', after: 1n }],
      syncParams: new RequestParameters({ sub: '' }, {}),
      syncRules: SYNC_RULES_GLOBAL,
      bucketStorage: storage
    });

    storage.updateTestChecksum({ bucket: 'global[]', checksum: 1, count: 1 });

    const line = (await state.buildNextCheckpointLine({
      base: { checkpoint: 1n, lsn: '1' },
      writeCheckpoint: null,
      update: CHECKPOINT_INVALIDATE_ALL
    }))!;
    expect(line.checkpointLine).toEqual({
      checkpoint: {
        buckets: [{ bucket: 'global[]', checksum: 1, count: 1, priority: 3 }],
        last_op_id: '1',
        write_checkpoint: undefined
      }
    });
    expect(line.bucketsToFetch).toEqual([
      {
        bucket: 'global[]',
        priority: 3
      }
    ]);
    expect(line.getFilteredBucketPositions()).toEqual(new Map([['global[]', 0n]]));
  });

  test('invalidating individual bucket', async () => {
    // We manually control the filter events here.

    const storage = new MockBucketChecksumStateStorage();
    // Set initial state
    storage.updateTestChecksum({ bucket: 'global[1]', checksum: 1, count: 1 });
    storage.updateTestChecksum({ bucket: 'global[2]', checksum: 1, count: 1 });

    const state = new BucketChecksumState({
      syncContext,
      syncParams: new RequestParameters({ sub: '' }, {}),
      syncRules: SYNC_RULES_GLOBAL_TWO,
      bucketStorage: storage
    });

    // We specifically do not set this here, so that we have manual control over the events.
    // storage.filter = state.checkpointFilter;

    const line = await state.buildNextCheckpointLine({
      base: { checkpoint: 1n, lsn: '1' },
      writeCheckpoint: null,
      update: CHECKPOINT_INVALIDATE_ALL
    });
    line!.advance();
    line!.updateBucketPosition({ bucket: 'global[1]', nextAfter: 1n, hasMore: false });
    line!.updateBucketPosition({ bucket: 'global[2]', nextAfter: 1n, hasMore: false });

    storage.updateTestChecksum({ bucket: 'global[1]', checksum: 2, count: 2 });
    storage.updateTestChecksum({ bucket: 'global[2]', checksum: 2, count: 2 });

    const line2 = (await state.buildNextCheckpointLine({
      base: { checkpoint: 2n, lsn: '2' },
      writeCheckpoint: null,
      update: {
        ...CHECKPOINT_INVALIDATE_ALL,
        // Invalidate the state for global[1] - will only re-check the single bucket.
        // This is essentially inconsistent state, but is the simplest way to test that
        // the filter is working.
        updatedDataBuckets: new Set(['global[1]']),
        invalidateDataBuckets: false
      }
    }))!;
    expect(line2.checkpointLine).toEqual({
      checkpoint_diff: {
        removed_buckets: [],
        updated_buckets: [
          // This does not include global[2], since it was not invalidated.
          { bucket: 'global[1]', checksum: 2, count: 2, priority: 3 }
        ],
        last_op_id: '2',
        write_checkpoint: undefined
      }
    });
    expect(line2.bucketsToFetch).toEqual([{ bucket: 'global[1]', priority: 3 }]);
  });

  test('invalidating all buckets', async () => {
    // We manually control the filter events here.
    const storage = new MockBucketChecksumStateStorage();

    const state = new BucketChecksumState({
      syncContext,
      syncParams: new RequestParameters({ sub: '' }, {}),
      syncRules: SYNC_RULES_GLOBAL_TWO,
      bucketStorage: storage
    });

    // We specifically do not set this here, so that we have manual control over the events.
    // storage.filter = state.checkpointFilter;

    // Set initial state
    storage.updateTestChecksum({ bucket: 'global[1]', checksum: 1, count: 1 });
    storage.updateTestChecksum({ bucket: 'global[2]', checksum: 1, count: 1 });

    const line = await state.buildNextCheckpointLine({
      base: { checkpoint: 1n, lsn: '1' },
      writeCheckpoint: null,
      update: CHECKPOINT_INVALIDATE_ALL
    });

    line!.advance();

    storage.updateTestChecksum({ bucket: 'global[1]', checksum: 2, count: 2 });
    storage.updateTestChecksum({ bucket: 'global[2]', checksum: 2, count: 2 });

    const line2 = (await state.buildNextCheckpointLine({
      base: { checkpoint: 2n, lsn: '2' },
      writeCheckpoint: null,
      // Invalidate the state - will re-check all buckets
      update: CHECKPOINT_INVALIDATE_ALL
    }))!;
    expect(line2.checkpointLine).toEqual({
      checkpoint_diff: {
        removed_buckets: [],
        updated_buckets: [
          { bucket: 'global[1]', checksum: 2, count: 2, priority: 3 },
          { bucket: 'global[2]', checksum: 2, count: 2, priority: 3 }
        ],
        last_op_id: '2',
        write_checkpoint: undefined
      }
    });
    expect(line2.bucketsToFetch).toEqual([
      { bucket: 'global[1]', priority: 3 },
      { bucket: 'global[2]', priority: 3 }
    ]);
  });

  test('interrupt and resume static buckets checkpoint', async () => {
    const storage = new MockBucketChecksumStateStorage();
    // Set intial state
    storage.updateTestChecksum({ bucket: 'global[1]', checksum: 3, count: 3 });
    storage.updateTestChecksum({ bucket: 'global[2]', checksum: 3, count: 3 });

    const state = new BucketChecksumState({
      syncContext,
      syncParams: new RequestParameters({ sub: '' }, {}),
      syncRules: SYNC_RULES_GLOBAL_TWO,
      bucketStorage: storage
    });

    const line = (await state.buildNextCheckpointLine({
      base: { checkpoint: 3n, lsn: '3' },
      writeCheckpoint: null,
      update: CHECKPOINT_INVALIDATE_ALL
    }))!;
    expect(line.checkpointLine).toEqual({
      checkpoint: {
        buckets: [
          { bucket: 'global[1]', checksum: 3, count: 3, priority: 3 },
          { bucket: 'global[2]', checksum: 3, count: 3, priority: 3 }
        ],
        last_op_id: '3',
        write_checkpoint: undefined
      }
    });
    expect(line.bucketsToFetch).toEqual([
      {
        bucket: 'global[1]',
        priority: 3
      },
      {
        bucket: 'global[2]',
        priority: 3
      }
    ]);

    // This is the bucket data to be fetched
    expect(line.getFilteredBucketPositions()).toEqual(
      new Map([
        ['global[1]', 0n],
        ['global[2]', 0n]
      ])
    );

    // No data changes here.
    // We simulate partial data sent, before a checkpoint is interrupted.
    line.advance();
    line.updateBucketPosition({ bucket: 'global[1]', nextAfter: 3n, hasMore: false });
    line.updateBucketPosition({ bucket: 'global[2]', nextAfter: 1n, hasMore: true });
    storage.updateTestChecksum({ bucket: 'global[1]', checksum: 4, count: 4 });

    const line2 = (await state.buildNextCheckpointLine({
      base: { checkpoint: 4n, lsn: '4' },
      writeCheckpoint: null,
      update: {
        ...CHECKPOINT_INVALIDATE_ALL,
        invalidateDataBuckets: false,
        updatedDataBuckets: new Set(['global[1]'])
      }
    }))!;
    expect(line2.checkpointLine).toEqual({
      checkpoint_diff: {
        removed_buckets: [],
        updated_buckets: [
          {
            bucket: 'global[1]',
            checksum: 4,
            count: 4,
            priority: 3
          }
        ],
        last_op_id: '4',
        write_checkpoint: undefined
      }
    });
    // This should contain both buckets, even though only one changed.
    expect(line2.bucketsToFetch).toEqual([
      {
        bucket: 'global[1]',
        priority: 3
      },
      {
        bucket: 'global[2]',
        priority: 3
      }
    ]);

    expect(line2.getFilteredBucketPositions()).toEqual(
      new Map([
        ['global[1]', 3n],
        ['global[2]', 1n]
      ])
    );
  });

  test('dynamic buckets with updates', async () => {
    const storage = new MockBucketChecksumStateStorage();
    // Set intial state
    storage.updateTestChecksum({ bucket: 'by_project[1]', checksum: 1, count: 1 });
    storage.updateTestChecksum({ bucket: 'by_project[2]', checksum: 1, count: 1 });
    storage.updateTestChecksum({ bucket: 'by_project[3]', checksum: 1, count: 1 });

    const state = new BucketChecksumState({
      syncContext,
      syncParams: new RequestParameters({ sub: 'u1' }, {}),
      syncRules: SYNC_RULES_DYNAMIC,
      bucketStorage: storage
    });

    storage.getParameterSets = async (
      checkpoint: InternalOpId,
      lookups: ParameterLookup[]
    ): Promise<SqliteJsonRow[]> => {
      expect(checkpoint).toEqual(1n);
      expect(lookups).toEqual([ParameterLookup.normalized('by_project', '1', ['u1'])]);
      return [{ id: 1 }, { id: 2 }];
    };

    const line = (await state.buildNextCheckpointLine({
      base: { checkpoint: 1n, lsn: '1' },
      writeCheckpoint: null,
      update: CHECKPOINT_INVALIDATE_ALL
    }))!;
    expect(line.checkpointLine).toEqual({
      checkpoint: {
        buckets: [
          { bucket: 'by_project[1]', checksum: 1, count: 1, priority: 3 },
          { bucket: 'by_project[2]', checksum: 1, count: 1, priority: 3 }
        ],
        last_op_id: '1',
        write_checkpoint: undefined
      }
    });
    expect(line.bucketsToFetch).toEqual([
      {
        bucket: 'by_project[1]',
        priority: 3
      },
      {
        bucket: 'by_project[2]',
        priority: 3
      }
    ]);
    // This is the bucket data to be fetched
    expect(line.getFilteredBucketPositions()).toEqual(
      new Map([
        ['by_project[1]', 0n],
        ['by_project[2]', 0n]
      ])
    );

    line.advance();
    line.updateBucketPosition({ bucket: 'by_project[1]', nextAfter: 1n, hasMore: false });
    line.updateBucketPosition({ bucket: 'by_project[2]', nextAfter: 1n, hasMore: false });

    storage.getParameterSets = async (
      checkpoint: InternalOpId,
      lookups: ParameterLookup[]
    ): Promise<SqliteJsonRow[]> => {
      expect(checkpoint).toEqual(2n);
      expect(lookups).toEqual([ParameterLookup.normalized('by_project', '1', ['u1'])]);
      return [{ id: 1 }, { id: 2 }, { id: 3 }];
    };

    // Now we get a new line
    const line2 = (await state.buildNextCheckpointLine({
      base: { checkpoint: 2n, lsn: '2' },
      writeCheckpoint: null,
      update: {
        invalidateDataBuckets: false,
        updatedDataBuckets: new Set(),
        updatedParameterLookups: new Set([JSONBig.stringify(['by_project', '1', 'u1'])]),
        invalidateParameterBuckets: false
      }
    }))!;
    expect(line2.checkpointLine).toEqual({
      checkpoint_diff: {
        removed_buckets: [],
        updated_buckets: [{ bucket: 'by_project[3]', checksum: 1, count: 1, priority: 3 }],
        last_op_id: '2',
        write_checkpoint: undefined
      }
    });
    expect(line2.getFilteredBucketPositions()).toEqual(new Map([['by_project[3]', 0n]]));
  });
});

class MockBucketChecksumStateStorage implements BucketChecksumStateStorage {
  private state: ChecksumMap = new Map();
  public filter?: (event: WatchFilterEvent) => boolean;

  constructor() {}

  updateTestChecksum(checksum: BucketChecksum): void {
    this.state.set(checksum.bucket, checksum);
    this.filter?.({ changedDataBucket: checksum.bucket });
  }

  invalidate() {
    this.filter?.({ invalidate: true });
  }

  async getChecksums(checkpoint: InternalOpId, buckets: string[]): Promise<ChecksumMap> {
    return new Map<string, BucketChecksum>(
      buckets.map((bucket) => {
        const checksum = this.state.get(bucket);
        return [
          bucket,
          {
            bucket: bucket,
            checksum: checksum?.checksum ?? 0,
            count: checksum?.count ?? 0
          }
        ];
      })
    );
  }

  async getParameterSets(checkpoint: InternalOpId, lookups: ParameterLookup[]): Promise<SqliteJsonRow[]> {
    throw new Error('Method not implemented.');
  }
}
