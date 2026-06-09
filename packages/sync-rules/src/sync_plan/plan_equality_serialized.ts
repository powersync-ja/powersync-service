import { Equality } from '../compiler/equality.js';
import {
  SerializedBucketDataSource,
  SerializedDataSource,
  SerializedParameterIndexLookupCreator
} from './serialize.js';

export interface SerializedBucketDataSourceWithDataSources {
  bucket: SerializedBucketDataSource;
  dataSources: readonly SerializedDataSource[];
}

/**
 * Equality for SerializedParameterIndexLookupCreator.
 *
 * This compares the JSON form directly. These are self-contained and use a stable serialization form, so it's
 * safe to compare this way.
 *
 * These are only considered equal if the lookupName remains the same, so may be affected by changing order of queries.
 */
export const serializedStreamParameterIndexLookupCreatorEquality =
  jsonEquality<SerializedParameterIndexLookupCreator>();

/**
 * SerializedBucketDataSource is not safe to compare _directly_, since it contains index references to SerializedDataSource
 * in the serialized sync plan. However, each SerializedDataSource is self-contained and safe to compare directly.
 *
 * So we compare the SerializedDataSource excluding `bucket.sources`, as well as the resolved SerializedDataSources (order independent).
 * The caller is responsible for resolving the SerializedDataSources.
 *
 * These are only considered equal if uniqueName is the same, so may be affected by changing order of joins/subqueries.
 */
export const serializedStreamBucketDataSourceEquality: Equality<SerializedBucketDataSourceWithDataSources> = {
  hash(hasher, value) {
    hasher.addString(JSON.stringify(bucketIdentity(value)));
  },
  equals(a, b) {
    return a === b || JSON.stringify(bucketIdentity(a)) == JSON.stringify(bucketIdentity(b));
  }
};

function bucketIdentity(value: SerializedBucketDataSourceWithDataSources) {
  const { bucket, dataSources } = value;

  return {
    hash: bucket.hash,
    uniqueName: bucket.uniqueName,
    // Sort so that the order of sources does not affect equality, as long as the same data sources are included.
    sources: bucket.sources.map((index) => JSON.stringify(dataSources[index])).sort()
  };
}

function jsonEquality<T>(): Equality<T> {
  return {
    hash(hasher, value) {
      hasher.addString(JSON.stringify(value));
    },
    equals(a, b) {
      return a === b || JSON.stringify(a) == JSON.stringify(b);
    }
  };
}
