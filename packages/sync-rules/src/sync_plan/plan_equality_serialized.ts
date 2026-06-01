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

export const serializedStreamDataSourceEquality: Equality<SerializedDataSource> = {
  hash(hasher, value) {
    hasher.addString(JSON.stringify(value));
  },
  equals(a, b) {
    return a === b || JSON.stringify(a) == JSON.stringify(b);
  }
};

export const serializedStreamParameterIndexLookupCreatorEquality: Equality<SerializedParameterIndexLookupCreator> = {
  hash(hasher, value) {
    hasher.addString(JSON.stringify(value));
  },
  equals(a, b) {
    return a === b || JSON.stringify(a) == JSON.stringify(b);
  }
};

export const serializedStreamBucketDataSourceEquality: Equality<SerializedBucketDataSourceWithDataSources> = {
  hash(hasher, value) {
    hasher.addString(serializedStreamBucketDataSourceKey(value));
  },
  equals(a, b) {
    return a === b || serializedStreamBucketDataSourceKey(a) == serializedStreamBucketDataSourceKey(b);
  }
};

function serializedStreamBucketDataSourceKey(value: SerializedBucketDataSourceWithDataSources): string {
  const { bucket, dataSources } = value;

  // Match StreamBucketDataSource's logical equality: bucket names are unique identifiers, not behavior.
  return JSON.stringify({
    hash: bucket.hash,
    sources: bucket.sources.map((index) => JSON.stringify(dataSources[index])).sort()
  });
}
