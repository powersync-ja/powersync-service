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

export const serializedStreamDataSourceEquality = jsonEquality<SerializedDataSource>();

export const serializedStreamParameterIndexLookupCreatorEquality =
  jsonEquality<SerializedParameterIndexLookupCreator>();

export const serializedStreamBucketDataSourceEquality: Equality<SerializedBucketDataSourceWithDataSources> = {
  hash(hasher, value) {
    hasher.addString(JSON.stringify(normalizeBucketDataSource(value)));
  },
  equals(a, b) {
    return a === b || JSON.stringify(normalizeBucketDataSource(a)) == JSON.stringify(normalizeBucketDataSource(b));
  }
};

function normalizeBucketDataSource(value: SerializedBucketDataSourceWithDataSources) {
  const { bucket, dataSources } = value;
  return {
    ...bucket,
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
