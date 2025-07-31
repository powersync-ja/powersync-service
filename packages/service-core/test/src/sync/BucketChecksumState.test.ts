import {
  BucketChecksum,
  BucketChecksumState,
  BucketChecksumStateOptions,
  BucketChecksumStateStorage,
  CHECKPOINT_INVALIDATE_ALL,
  ChecksumMap,
  InternalOpId,
  ReplicationCheckpoint,
  StreamingSyncRequest,
  SyncContext,
  WatchFilterEvent
} from '@/index.js';
import { JSONBig } from '@powersync/service-jsonbig';
import {
  SqliteJsonRow,
  ParameterLookup,
  SqlSyncRules,
  RequestJwtPayload,
  BucketSource,
  BucketSourceType,
  BucketParameterQuerier
} from '@powersync/service-sync-rules';
import { describe, expect, test, beforeEach } from 'vitest';

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

  const syncRequest: StreamingSyncRequest = {};
  const tokenPayload: RequestJwtPayload = { sub: '' };

  test('global bucket with update', async () => {
    const storage = new MockBucketChecksumStateStorage();
    // Set intial state
    storage.updateTestChecksum({ bucket: 'global[]', checksum: 1, count: 1 });

    const state = new BucketChecksumState({
      syncContext,
      syncRequest,
      tokenPayload,
      syncRules: SYNC_RULES_GLOBAL,
      bucketStorage: storage
    });

    const line = (await state.buildNextCheckpointLine({
      base: storage.makeCheckpoint(1n),
      writeCheckpoint: null,
      update: CHECKPOINT_INVALIDATE_ALL
    }))!;
    line.advance();
    expect(line.checkpointLine).toEqual({
      checkpoint: {
        buckets: [{ bucket: 'global[]', checksum: 1, count: 1, priority: 3, subscriptions: [{ default: 0 }] }],
        last_op_id: '1',
        write_checkpoint: undefined,
        streams: [{ name: 'global', is_default: true }]
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
      base: storage.makeCheckpoint(2n),
      writeCheckpoint: null,
      update: {
        updatedDataBuckets: new Set(['global[]']),
        invalidateDataBuckets: false,
        updatedParameterLookups: new Set(),
        invalidateParameterBuckets: false
      }
    }))!;
    line2.advance();
    expect(line2.checkpointLine).toEqual({
      checkpoint_diff: {
        removed_buckets: [],
        updated_buckets: [{ bucket: 'global[]', checksum: 2, count: 2, priority: 3, subscriptions: [{ default: 0 }] }],
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
      tokenPayload,
      // Client sets the initial state here
      syncRequest: { buckets: [{ name: 'global[]', after: '1' }] },
      syncRules: SYNC_RULES_GLOBAL,
      bucketStorage: storage
    });

    const line = (await state.buildNextCheckpointLine({
      base: storage.makeCheckpoint(1n),
      writeCheckpoint: null,
      update: CHECKPOINT_INVALIDATE_ALL
    }))!;
    line.advance();
    expect(line.checkpointLine).toEqual({
      checkpoint: {
        buckets: [{ bucket: 'global[]', checksum: 1, count: 1, priority: 3, subscriptions: [{ default: 0 }] }],
        last_op_id: '1',
        write_checkpoint: undefined,
        streams: [{ name: 'global', is_default: true }]
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
      tokenPayload,
      syncRequest,
      syncRules: SYNC_RULES_GLOBAL_TWO,
      bucketStorage: storage
    });

    const line = (await state.buildNextCheckpointLine({
      base: storage.makeCheckpoint(1n),
      writeCheckpoint: null,
      update: CHECKPOINT_INVALIDATE_ALL
    }))!;
    expect(line.checkpointLine).toEqual({
      checkpoint: {
        buckets: [
          { bucket: 'global[1]', checksum: 1, count: 1, priority: 3, subscriptions: [{ default: 0 }] },
          { bucket: 'global[2]', checksum: 1, count: 1, priority: 3, subscriptions: [{ default: 0 }] }
        ],
        last_op_id: '1',
        write_checkpoint: undefined,
        streams: [{ name: 'global', is_default: true }]
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
      base: storage.makeCheckpoint(2n),
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
          { bucket: 'global[1]', checksum: 2, count: 2, priority: 3, subscriptions: [{ default: 0 }] },
          { bucket: 'global[2]', checksum: 2, count: 2, priority: 3, subscriptions: [{ default: 0 }] }
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
      tokenPayload,
      // Client sets the initial state here
      syncRequest: { buckets: [{ name: 'something_here[]', after: '1' }] },
      syncRules: SYNC_RULES_GLOBAL,
      bucketStorage: storage
    });

    storage.updateTestChecksum({ bucket: 'global[]', checksum: 1, count: 1 });

    const line = (await state.buildNextCheckpointLine({
      base: storage.makeCheckpoint(1n),
      writeCheckpoint: null,
      update: CHECKPOINT_INVALIDATE_ALL
    }))!;
    line.advance();
    expect(line.checkpointLine).toEqual({
      checkpoint: {
        buckets: [{ bucket: 'global[]', checksum: 1, count: 1, priority: 3, subscriptions: [{ default: 0 }] }],
        last_op_id: '1',
        write_checkpoint: undefined,
        streams: [{ name: 'global', is_default: true }]
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
      tokenPayload,
      syncRequest,
      syncRules: SYNC_RULES_GLOBAL_TWO,
      bucketStorage: storage
    });

    // We specifically do not set this here, so that we have manual control over the events.
    // storage.filter = state.checkpointFilter;

    const line = await state.buildNextCheckpointLine({
      base: storage.makeCheckpoint(1n),
      writeCheckpoint: null,
      update: CHECKPOINT_INVALIDATE_ALL
    });
    line!.advance();
    line!.updateBucketPosition({ bucket: 'global[1]', nextAfter: 1n, hasMore: false });
    line!.updateBucketPosition({ bucket: 'global[2]', nextAfter: 1n, hasMore: false });

    storage.updateTestChecksum({ bucket: 'global[1]', checksum: 2, count: 2 });
    storage.updateTestChecksum({ bucket: 'global[2]', checksum: 2, count: 2 });

    const line2 = (await state.buildNextCheckpointLine({
      base: storage.makeCheckpoint(2n),
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
          { bucket: 'global[1]', checksum: 2, count: 2, priority: 3, subscriptions: [{ default: 0 }] }
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
      tokenPayload,
      syncRequest,
      syncRules: SYNC_RULES_GLOBAL_TWO,
      bucketStorage: storage
    });

    // We specifically do not set this here, so that we have manual control over the events.
    // storage.filter = state.checkpointFilter;

    // Set initial state
    storage.updateTestChecksum({ bucket: 'global[1]', checksum: 1, count: 1 });
    storage.updateTestChecksum({ bucket: 'global[2]', checksum: 1, count: 1 });

    const line = await state.buildNextCheckpointLine({
      base: storage.makeCheckpoint(1n),
      writeCheckpoint: null,
      update: CHECKPOINT_INVALIDATE_ALL
    });

    line!.advance();

    storage.updateTestChecksum({ bucket: 'global[1]', checksum: 2, count: 2 });
    storage.updateTestChecksum({ bucket: 'global[2]', checksum: 2, count: 2 });

    const line2 = (await state.buildNextCheckpointLine({
      base: storage.makeCheckpoint(2n),
      writeCheckpoint: null,
      // Invalidate the state - will re-check all buckets
      update: CHECKPOINT_INVALIDATE_ALL
    }))!;
    expect(line2.checkpointLine).toEqual({
      checkpoint_diff: {
        removed_buckets: [],
        updated_buckets: [
          { bucket: 'global[1]', checksum: 2, count: 2, priority: 3, subscriptions: [{ default: 0 }] },
          { bucket: 'global[2]', checksum: 2, count: 2, priority: 3, subscriptions: [{ default: 0 }] }
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
      tokenPayload,
      syncRequest,
      syncRules: SYNC_RULES_GLOBAL_TWO,
      bucketStorage: storage
    });

    const line = (await state.buildNextCheckpointLine({
      base: storage.makeCheckpoint(3n),
      writeCheckpoint: null,
      update: CHECKPOINT_INVALIDATE_ALL
    }))!;
    line.advance();
    expect(line.checkpointLine).toEqual({
      checkpoint: {
        buckets: [
          { bucket: 'global[1]', checksum: 3, count: 3, priority: 3, subscriptions: [{ default: 0 }] },
          { bucket: 'global[2]', checksum: 3, count: 3, priority: 3, subscriptions: [{ default: 0 }] }
        ],
        last_op_id: '3',
        write_checkpoint: undefined,
        streams: [{ name: 'global', is_default: true }]
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
      base: storage.makeCheckpoint(4n),
      writeCheckpoint: null,
      update: {
        ...CHECKPOINT_INVALIDATE_ALL,
        invalidateDataBuckets: false,
        updatedDataBuckets: new Set(['global[1]'])
      }
    }))!;
    line2.advance();
    expect(line2.checkpointLine).toEqual({
      checkpoint_diff: {
        removed_buckets: [],
        updated_buckets: [
          {
            bucket: 'global[1]',
            checksum: 4,
            count: 4,
            priority: 3,
            subscriptions: [{ default: 0 }]
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
      tokenPayload: { sub: 'u1' },
      syncRequest,
      syncRules: SYNC_RULES_DYNAMIC,
      bucketStorage: storage
    });

    const line = (await state.buildNextCheckpointLine({
      base: storage.makeCheckpoint(1n, (lookups) => {
        expect(lookups).toEqual([ParameterLookup.normalized('by_project', '1', ['u1'])]);
        return [{ id: 1 }, { id: 2 }];
      }),
      writeCheckpoint: null,
      update: CHECKPOINT_INVALIDATE_ALL
    }))!;
    expect(line.checkpointLine).toEqual({
      checkpoint: {
        buckets: [
          {
            bucket: 'by_project[1]',
            checksum: 1,
            count: 1,
            priority: 3,
            subscriptions: [{ default: 0 }]
          },
          {
            bucket: 'by_project[2]',
            checksum: 1,
            count: 1,
            priority: 3,
            subscriptions: [{ default: 0 }]
          }
        ],
        last_op_id: '1',
        streams: [
          {
            is_default: true,
            name: 'by_project'
          }
        ],
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
    line.advance();
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

    // Now we get a new line
    const line2 = (await state.buildNextCheckpointLine({
      base: storage.makeCheckpoint(2n, (lookups) => {
        expect(lookups).toEqual([ParameterLookup.normalized('by_project', '1', ['u1'])]);
        return [{ id: 1 }, { id: 2 }, { id: 3 }];
      }),
      writeCheckpoint: null,
      update: {
        invalidateDataBuckets: false,
        updatedDataBuckets: new Set(),
        updatedParameterLookups: new Set([JSONBig.stringify(['by_project', '1', 'u1'])]),
        invalidateParameterBuckets: false
      }
    }))!;
    line2.advance();
    expect(line2.checkpointLine).toEqual({
      checkpoint_diff: {
        removed_buckets: [],
        updated_buckets: [
          {
            bucket: 'by_project[3]',
            checksum: 1,
            count: 1,
            priority: 3,
            subscriptions: [{ default: 0 }]
          }
        ],
        last_op_id: '2',
        write_checkpoint: undefined
      }
    });
    expect(line2.getFilteredBucketPositions()).toEqual(new Map([['by_project[3]', 0n]]));
  });

  describe('streams', () => {
    let source: { -readonly [P in keyof BucketSource]: BucketSource[P] };
    let storage: MockBucketChecksumStateStorage;
    let staticBucketIds = ['stream|0[]'];

    function checksumState(options?: Partial<BucketChecksumStateOptions>) {
      const rules = new SqlSyncRules('');
      rules.bucketSources.push(source);

      return new BucketChecksumState({
        syncContext,
        syncRequest,
        tokenPayload,
        syncRules: rules,
        bucketStorage: storage,
        ...options
      });
    }

    function createQuerier(ids: string[], subscription: string | null): BucketParameterQuerier {
      return {
        staticBuckets: ids.map((bucket) => ({
          definition: 'stream',
          inclusion_reasons: subscription == null ? ['default'] : [{ subscription }],
          bucket,
          priority: 3
        })),
        hasDynamicBuckets: false,
        parameterQueryLookups: [],
        queryDynamicBucketDescriptions: function (): never {
          throw new Error('no dynamic buckets.');
        }
      };
    }

    beforeEach(() => {
      // Currently using mocked streams before streams are actually implemented as parsable rules.
      source = {
        name: 'stream',
        type: BucketSourceType.SYNC_STREAM,
        subscribedToByDefault: false,
        pushBucketParameterQueriers(result, options) {
          // Create a fake querier that resolves teh global stream["default"] bucket by default and allows extracting
          // additional buckets from parameters.
          const subscriptions = options.streams['stream'] ?? [];
          if (!this.subscribedToByDefault && !subscriptions.length) {
            return;
          }

          let hasExplicitDefaultSubscription = false;
          for (const subscription of subscriptions) {
            let subscriptionParameters = [];

            if (subscription.parameters != null) {
              subscriptionParameters = JSON.parse(subscription.parameters['ids'] as string).map(
                (e: string) => `stream["${e}"]`
              );
            } else {
              hasExplicitDefaultSubscription = true;
            }

            result.push(createQuerier([...subscriptionParameters], subscription.opaque_id));
          }

          // If the stream is subscribed to by default and there is no explicit subscription that would match the default
          // subscription, also include the default querier.
          if (this.subscribedToByDefault && !hasExplicitDefaultSubscription) {
            result.push(createQuerier(['stream["default"]'], null));
          }
        }
      } satisfies Partial<BucketSource> as any;

      storage = new MockBucketChecksumStateStorage();
      storage.updateTestChecksum({ bucket: 'stream["default"]', checksum: 1, count: 1 });
      storage.updateTestChecksum({ bucket: 'stream["a"]', checksum: 1, count: 1 });
      storage.updateTestChecksum({ bucket: 'stream["b"]', checksum: 1, count: 1 });
    });

    test('includes defaults', async () => {
      source.subscribedToByDefault = true;
      const state = checksumState();

      const line = await state.buildNextCheckpointLine({
        base: { checkpoint: 1n, lsn: '1' },
        writeCheckpoint: null,
        update: CHECKPOINT_INVALIDATE_ALL
      })!;
      line?.advance();
      expect(line?.checkpointLine).toEqual({
        checkpoint: {
          buckets: [
            { bucket: 'stream["default"]', checksum: 1, count: 1, priority: 3, subscriptions: [{ default: 0 }] }
          ],
          last_op_id: '1',
          write_checkpoint: undefined,
          streams: [{ name: 'stream', is_default: true }]
        }
      });
    });

    test('can exclude defaults', async () => {
      source.subscribedToByDefault = true;
      const state = checksumState({ syncRequest: { streams: { include_defaults: false, subscriptions: [] } } });

      const line = await state.buildNextCheckpointLine({
        base: { checkpoint: 1n, lsn: '1' },
        writeCheckpoint: null,
        update: CHECKPOINT_INVALIDATE_ALL
      })!;
      line?.advance();
      expect(line?.checkpointLine).toEqual({
        checkpoint: {
          buckets: [],
          last_op_id: '1',
          write_checkpoint: undefined,
          streams: []
        }
      });
    });

    test('custom subscriptions', async () => {
      source.subscribedToByDefault = true;

      const state = checksumState({
        syncRequest: {
          streams: {
            subscriptions: [
              { stream: 'stream', client_id: '1', parameters: { ids: '["a"]' } },
              { stream: 'stream', client_id: '2', parameters: { ids: '["b"]' }, override_priority: 1 }
            ]
          }
        }
      });

      const line = await state.buildNextCheckpointLine({
        base: { checkpoint: 1n, lsn: '1' },
        writeCheckpoint: null,
        update: CHECKPOINT_INVALIDATE_ALL
      })!;
      line?.advance();
      expect(line?.checkpointLine).toEqual({
        checkpoint: {
          buckets: [
            { bucket: 'stream["a"]', checksum: 1, count: 1, priority: 3, subscriptions: ['1'] },
            { bucket: 'stream["b"]', checksum: 1, count: 1, priority: 1, subscriptions: ['2'] },
            { bucket: 'stream["default"]', checksum: 1, count: 1, priority: 3, subscriptions: [{ default: 0 }] }
          ],
          last_op_id: '1',
          write_checkpoint: undefined,
          streams: [{ name: 'stream', is_default: true }]
        }
      });
    });

    test('overlap between custom subscriptions', async () => {
      const state = checksumState({
        syncRequest: {
          streams: {
            subscriptions: [
              { stream: 'stream', client_id: '1', parameters: { ids: '["a", "b"]' } },
              { stream: 'stream', client_id: '2', parameters: { ids: '["b"]' }, override_priority: 1 }
            ]
          }
        }
      });

      const line = await state.buildNextCheckpointLine({
        base: { checkpoint: 1n, lsn: '1' },
        writeCheckpoint: null,
        update: CHECKPOINT_INVALIDATE_ALL
      })!;
      line?.advance();
      expect(line?.checkpointLine).toEqual({
        checkpoint: {
          buckets: [
            { bucket: 'stream["a"]', checksum: 1, count: 1, priority: 3, subscriptions: ['1'] },
            { bucket: 'stream["b"]', checksum: 1, count: 1, priority: 1, subscriptions: ['1', '2'] }
          ],
          last_op_id: '1',
          write_checkpoint: undefined,
          streams: [{ name: 'stream', is_default: false }]
        }
      });
    });

    test('overlap between default and custom subscription', async () => {
      source.subscribedToByDefault = true;
      const state = checksumState({
        syncRequest: {
          streams: {
            subscriptions: [
              { stream: 'stream', client_id: '1', parameters: { ids: '["a", "default"]' }, override_priority: 1 }
            ]
          }
        }
      });

      const line = await state.buildNextCheckpointLine({
        base: { checkpoint: 1n, lsn: '1' },
        writeCheckpoint: null,
        update: CHECKPOINT_INVALIDATE_ALL
      })!;
      line?.advance();
      expect(line?.checkpointLine).toEqual({
        checkpoint: {
          buckets: [
            { bucket: 'stream["a"]', checksum: 1, count: 1, priority: 1, subscriptions: ['1'] },
            { bucket: 'stream["default"]', checksum: 1, count: 1, priority: 1, subscriptions: ['1', { default: 0 }] }
          ],
          last_op_id: '1',
          write_checkpoint: undefined,
          streams: [{ name: 'stream', is_default: true }]
        }
      });
    });
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

  makeCheckpoint(
    opId: InternalOpId,
    parameters?: (lookups: ParameterLookup[]) => SqliteJsonRow[]
  ): ReplicationCheckpoint {
    return {
      checkpoint: opId,
      lsn: String(opId),
      getParameterSets: async (lookups: ParameterLookup[]) => {
        if (parameters == null) {
          throw new Error(`getParametersSets not defined for checkpoint ${opId}`);
        }
        return parameters(lookups);
      }
    };
  }
}
