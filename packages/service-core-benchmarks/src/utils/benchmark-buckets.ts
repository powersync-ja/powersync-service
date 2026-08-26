import { storage } from '@powersync/service-core';
import {
  BaseJwtPayload,
  HydratedSyncConfig,
  mergeBuckets,
  RequestParameters,
  ResolvedBucket
} from '@powersync/service-sync-rules';

const BENCHMARK_USER_ID = 'benchmark-user';
const PARAMETER_RESULT_LIMIT = 1_000;

export interface ResolveBenchmarkBucketsOptions {
  readonly syncRules: HydratedSyncConfig;
  readonly checkpoint: storage.ReplicationCheckpoint;
  readonly syncParameters: Record<string, unknown>;
}

export async function resolveBenchmarkBuckets(options: ResolveBenchmarkBucketsOptions): Promise<ResolvedBucket[]> {
  const globalParameters = new RequestParameters(
    new BaseJwtPayload({ sub: BENCHMARK_USER_ID }),
    options.syncParameters
  );

  const { querier, errors } = options.syncRules.getBucketParameterQuerier({
    globalParameters,
    hasDefaultStreams: true,
    streams: {}
  });

  if (errors.length > 0) {
    throw new Error(
      `Benchmark sync parameters could not resolve buckets: ${errors.map((error) => error.message).join('; ')}`
    );
  }

  let remainingParameterResults = PARAMETER_RESULT_LIMIT;
  const dynamicBuckets = querier.hasDynamicBuckets
    ? await querier.queryDynamicBucketDescriptions({
        getParameterSets: async (lookups) => {
          const result = await options.checkpoint.getParameterSets(lookups, remainingParameterResults);
          remainingParameterResults -= result.reduce((count, item) => count + item.rows.length, 0);
          return result;
        }
      })
    : [];

  return mergeBuckets([...querier.staticBuckets, ...dynamicBuckets]);
}

export function bucketRequests(buckets: readonly ResolvedBucket[]): storage.BucketDataRequest[] {
  return buckets.map((bucket) => ({ bucket: bucket.bucket, start: 0n, source: bucket.source }));
}
