import { BucketChecksum, BucketChecksumState, BucketChecksumStateStorage } from '@/index.js';
import { RequestParameters, SqlSyncRules } from '@powersync/service-sync-rules';
import { describe, expect, test } from 'vitest';

describe('BucketChecksumState', () => {
  test('global bucket', async () => {
    const storage: BucketChecksumStateStorage = {
      async getChecksums(checkpoint, buckets) {
        return new Map<string, BucketChecksum>(
          buckets.map((b, i) => {
            return [
              b,
              {
                bucket: b,
                checksum: Number(checkpoint),
                count: i
              }
            ];
          })
        );
      },
      getParameterSets(checkpoint, lookups) {
        throw new Error('Method not implemented.');
      }
    };

    const syncRules = SqlSyncRules.fromYaml(
      `
bucket_definitions:
  global:
    data: []
      `,
      { defaultSchema: 'public' }
    );
    const state = new BucketChecksumState({
      initialBucketState: new Map(),
      syncParams: new RequestParameters({ sub: '' }, {}),
      syncRules: syncRules,
      bucketStorage: storage
    });

    const line = (await state.buildNextCheckpointLine({ base: { checkpoint: '1', lsn: '1' }, writeCheckpoint: null }))!;
    expect(line.checkpointLine).toEqual({
      checkpoint: {
        buckets: [{ bucket: 'global[]', checksum: 1, count: 0, priority: 3 }],
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

    state.checkpointFilter({
      invalidate: true
    });

    const line2 = (await state.buildNextCheckpointLine({
      base: { checkpoint: '2', lsn: '2' },
      writeCheckpoint: null
    }))!;
    expect(line2.checkpointLine).toEqual({
      checkpoint_diff: {
        removed_buckets: [],
        updated_buckets: [{ bucket: 'global[]', checksum: 2, count: 0, priority: 3 }],
        last_op_id: '2',
        write_checkpoint: undefined
      }
    });
  });
});
