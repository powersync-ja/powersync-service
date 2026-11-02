import {
  BucketChecksumState,
  CHECKPOINT_INVALIDATE_ALL,
  ChecksumCache,
  ChecksumMap,
  FetchPartialBucketChecksum,
  JwtPayload,
  RequestTracker,
  streamResponse,
  SyncRulesBucketStorage
} from '@/index.js';
import { DEFAULT_HYDRATION_STATE, HydratedSyncConfig, nodeSqlite, SqlSyncRules } from '@powersync/service-sync-rules';
import * as sqlite from 'node:sqlite';
import { describe, expect, test, vi } from 'vitest';
import { mockServiceContext } from '../routes/mocks.js';

const syncRules = SqlSyncRules.fromYaml(
  `
config:
  edition: 3
streams:
  test:
    query: SELECT * FROM test
`,
  { defaultSchema: 'public' }
).config.hydrate({ hydrationState: DEFAULT_HYDRATION_STATE, sqlite: nodeSqlite(sqlite) });

function response(
  storage: Partial<SyncRulesBucketStorage>,
  signal: AbortSignal,
  config: HydratedSyncConfig = syncRules
) {
  const context = mockServiceContext(storage);
  return streamResponse({
    syncContext: context.syncContext,
    bucketStorage: storage as SyncRulesBucketStorage,
    syncRules: config,
    params: { streams: { subscriptions: [{ stream: 'test', parameters: null, override_priority: null }] } },
    token: new JwtPayload({ sub: 'test-user', exp: Date.now() / 1000 + 3600 }),
    tracker: new RequestTracker(context.metricsEngine),
    isEncodingAsBson: false,
    signal
  })[Symbol.asyncIterator]();
}

describe('streamResponse cancellation', () => {
  test('preserves a checksum failure before cancellation', async () => {
    const failure = new Error('checksum query failed');
    const iterator = response(
      {
        watchCheckpointChanges: async function* () {
          yield {
            base: { checkpoint: 1n, lsn: null, getParameterSets: vi.fn() },
            writeCheckpoint: null,
            update: CHECKPOINT_INVALIDATE_ALL
          };
        },
        getChecksums: async () => {
          throw failure;
        }
      },
      new AbortController().signal
    );
    await expect(iterator.next()).rejects.toBe(failure);
  });

  test('a data fetch failure cancels an overlapping checkpoint calculation', async () => {
    const controller = new AbortController();
    const started = Promise.withResolvers<void>();
    const fetch = Promise.withResolvers<ChecksumMap>();
    const failure = new Error('data fetch failed');
    let closed = false;
    const iterator = response(
      {
        watchCheckpointChanges: async function* () {
          try {
            for (const checkpoint of [1n, 2n]) {
              yield {
                base: { checkpoint, lsn: null, getParameterSets: vi.fn() },
                writeCheckpoint: null,
                update: CHECKPOINT_INVALIDATE_ALL
              };
            }
          } finally {
            closed = true;
          }
        },
        getChecksums: async (checkpoint, buckets) => {
          if (checkpoint.checkpoint === 2n) {
            started.resolve();
            return fetch.promise;
          }
          return new Map(buckets.map(({ bucket }) => [bucket, { bucket, checksum: 1, count: 1000 }]));
        },
        getBucketDataBatch: async function* (_checkpoint, buckets) {
          yield {
            chunkData: {
              bucket: buckets[0].bucket,
              data: Array.from({ length: 1000 }, (_, i) => ({ op: 'PUT' as const, op_id: String(i + 1), checksum: 1 })),
              has_more: true,
              after: '0',
              next_after: '1000'
            },
            targetOp: null
          };
          await started.promise;
          throw failure;
        }
      },
      controller.signal
    );
    const consume = async () => {
      while (!(await iterator.next()).done) {}
    };
    let finished = false;
    const completion = expect(
      consume().finally(() => {
        finished = true;
      })
    ).rejects.toBe(failure);
    await started.promise;
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(finished).toBe(false);
    fetch.resolve(new Map());
    await completion;
    expect(controller.signal.aborted).toBe(false);
    expect(closed).toBe(true);
  });

  test('checks cancellation after an incremental checksum read finishes', async () => {
    const controller = new AbortController();
    const started = Promise.withResolvers<void>();
    const fetch = Promise.withResolvers<ChecksumMap>();
    const getChecksums = vi
      .fn<SyncRulesBucketStorage['getChecksums']>()
      .mockImplementation(
        async (_cp, buckets) => new Map(buckets.map(({ bucket }) => [bucket, { bucket, checksum: 1, count: 1 }]))
      );
    const context = mockServiceContext({});
    const state = new BucketChecksumState({
      syncContext: context.syncContext,
      bucketStorage: { getChecksums },
      syncRules,
      syncRequest: { streams: { subscriptions: [{ stream: 'test', parameters: null, override_priority: null }] } },
      tokenPayload: new JwtPayload({ sub: 'test-user' })
    });
    const base = { checkpoint: 1n, lsn: null, getParameterSets: vi.fn() };
    const first = (await state.buildNextCheckpointLine({
      base,
      writeCheckpoint: null,
      update: CHECKPOINT_INVALIDATE_ALL
    }))!;
    first.advance();
    getChecksums.mockImplementation(() => {
      started.resolve();
      return fetch.promise;
    });

    const pending = state.buildNextCheckpointLine(
      {
        base: { ...base, checkpoint: 2n },
        writeCheckpoint: null,
        update: {
          ...CHECKPOINT_INVALIDATE_ALL,
          invalidateDataBuckets: false,
          updatedDataBuckets: new Set(first.bucketsToFetch.map((b) => b.bucket))
        }
      },
      undefined,
      controller.signal
    );
    const reason = new Error('client disconnected during checkpoint diff');
    const completion = expect(pending).rejects.toBe(reason);
    await started.promise;
    controller.abort(reason);
    fetch.resolve(new Map());
    await completion;
  });

  test('finishes a pending parameter read before cancellation, without starting checksum work', async () => {
    const config = SqlSyncRules.fromYaml(
      `
config:
  edition: 3
streams:
  test:
    query: SELECT * FROM test WHERE id IN (SELECT id FROM memberships WHERE user_id = auth.user_id())
`,
      { defaultSchema: 'public' }
    ).config.hydrate({ hydrationState: DEFAULT_HYDRATION_STATE, sqlite: nodeSqlite(sqlite) });
    const controller = new AbortController();
    const started = Promise.withResolvers<void>();
    const read = Promise.withResolvers<never[]>();
    const getChecksums = vi.fn();
    let closed = false;
    const iterator = response(
      {
        watchCheckpointChanges: async function* () {
          try {
            yield {
              base: {
                checkpoint: 1n,
                lsn: null,
                getParameterSets: () => {
                  started.resolve();
                  return read.promise;
                }
              },
              writeCheckpoint: null,
              update: CHECKPOINT_INVALIDATE_ALL
            };
          } finally {
            closed = true;
          }
        },
        getChecksums
      },
      controller.signal,
      config
    );
    const next = iterator.next();
    await started.promise;
    controller.abort();
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(closed).toBe(false);
    read.resolve([]);
    await expect(next).resolves.toEqual({ done: true, value: undefined });
    expect(closed).toBe(true);
    expect(getChecksums).not.toHaveBeenCalled();
  });

  test.each(['success', 'failure'] as const)(
    'waits for a shared checksum fetch to finish with %s before closing',
    async (outcome) => {
      const started = Promise.withResolvers<void>();
      const fetch = Promise.withResolvers<ChecksumMap>();
      const fetchChecksums = vi.fn((_batch: FetchPartialBucketChecksum[]) => fetch.promise);
      const cache = new ChecksumCache({ fetchChecksums });
      const controller = new AbortController();
      const checkpoint = { checkpoint: 1n, lsn: null, getParameterSets: vi.fn() };
      let closed = false;
      let shared: Promise<PromiseSettledResult<ChecksumMap>>;
      const iterator = response(
        {
          watchCheckpointChanges: async function* () {
            try {
              yield { base: checkpoint, writeCheckpoint: null, update: CHECKPOINT_INVALIDATE_ALL };
            } finally {
              closed = true;
            }
          },
          getChecksums: (_checkpoint, buckets) => {
            const first = cache.getChecksumMap(1n, buckets);
            // A second connection shares this fetch and must survive the first disconnecting.
            shared = Promise.allSettled([cache.getChecksumMap(1n, buckets)]).then(([result]) => result);
            started.resolve();
            return first;
          }
        },
        controller.signal
      );

      const next = Promise.allSettled([iterator.next()]).then(([result]) => result);
      await started.promise;
      controller.abort(new Error('client disconnected'));
      // In-flight storage work finishes normally before cancellation is observed.
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(closed).toBe(false);
      expect(fetchChecksums).toHaveBeenCalledTimes(1);

      if (outcome === 'success') {
        const bucket = fetchChecksums.mock.calls[0][0][0].bucket;
        const checksums = new Map([[bucket, { bucket, checksum: 42, count: 1 }]]);
        fetch.resolve(checksums);
        await expect(shared!).resolves.toEqual({ status: 'fulfilled', value: checksums });
        await expect(next).resolves.toEqual({ status: 'fulfilled', value: { done: true, value: undefined } });
      } else {
        const error = new Error('checksum query failed after disconnect');
        fetch.reject(error);
        await expect(shared!).resolves.toEqual({ status: 'rejected', reason: error });
        const result = await next;
        expect(result.status).toBe('rejected');
        if (result.status === 'rejected') {
          const errors = result.reason instanceof AggregateError ? result.reason.errors : [result.reason];
          expect(errors).toContain(error);
        }
      }
      expect(closed).toBe(true);
    }
  );

  test.each([
    { reason: new Error('client disconnected') },
    { reason: 'client disconnected' },
    { reason: 0 },
    { reason: false },
    { reason: '' },
    { reason: null },
    { reason: NaN },
    { reason: undefined }
  ])('ends without error for abort reason $reason, before and during a read', async ({ reason }) => {
    const watch = vi.fn(async function* () {});
    const preAborted = response({ watchCheckpointChanges: watch }, AbortSignal.abort(reason));
    await expect(preAborted.next()).resolves.toEqual({ done: true, value: undefined });
    expect(watch).not.toHaveBeenCalled();

    const started = Promise.withResolvers<void>();
    const controller = new AbortController();
    let closed = false;
    const iterator = response(
      {
        watchCheckpointChanges: async function* ({ signal }) {
          try {
            started.resolve();
            await new Promise<void>((_resolve, reject) => {
              signal.addEventListener('abort', () => reject(signal.reason), { once: true });
            });
          } finally {
            closed = true;
          }
        }
      },
      controller.signal
    );
    const next = iterator.next();
    const completion = expect(next).resolves.toEqual({ done: true, value: undefined });
    await started.promise;
    controller.abort(reason);
    await completion;
    expect(closed).toBe(true);
  });

  test('does not suppress cleanup failures alongside cancellation', async () => {
    const started = Promise.withResolvers<void>();
    const controller = new AbortController();
    const reason = new Error('client disconnected');
    const cleanupError = new Error('watcher cleanup failed');
    const iterator = response(
      {
        watchCheckpointChanges: async function* ({ signal }) {
          try {
            started.resolve();
            await new Promise<void>((_resolve, reject) => {
              signal.addEventListener('abort', () => reject(signal.reason), { once: true });
            });
          } finally {
            throw cleanupError;
          }
        }
      },
      controller.signal
    );
    const outcome = iterator.next().catch((error) => error);
    await started.promise;
    controller.abort(reason);
    const error = await outcome;
    expect(error).toBeInstanceOf(AggregateError);
    const failures = error.errors.flatMap((failure: unknown) =>
      failure instanceof AggregateError ? failure.errors : [failure]
    );
    expect(failures).toContain(cleanupError);
  });

  test('does not suppress an unrelated failure when the signal is also aborted', async () => {
    const controller = new AbortController();
    const failure = new Error('storage failed');
    const iterator = response(
      {
        watchCheckpointChanges: async function* () {
          try {
            throw failure;
          } finally {
            // The source error starts shutdown before the caller cancels in the same turn.
            queueMicrotask(() => controller.abort(new Error('client disconnected')));
          }
        }
      },
      controller.signal
    );
    const outcome = await iterator.next().catch((error) => error);
    expect(controller.signal.aborted).toBe(true);
    expect(outcome).toBeInstanceOf(Error);
    if (outcome instanceof AggregateError) {
      expect(outcome.errors).toContain(failure);
    } else {
      expect(outcome).toBe(failure);
    }
  });
});
