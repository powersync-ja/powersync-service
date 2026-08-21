import type { Equality } from '../compiler/equality.js';
import type {
  SerializedBucketDataSource,
  SerializedDataSource,
  SerializedEventDescriptor,
  SerializedParameterIndexLookupCreator
} from './serialize.js';

export interface SerializedBucketDataSourceWithDataSources {
  bucket: SerializedBucketDataSource;
  dataSources: readonly SerializedDataSource[];
}

/**
 * Structural equality for a compiled event definition.
 *
 * This decides whether an event in a new sync config matches one in an active config, so it can keep that config's
 * assigned storage id during incremental reprocessing instead of being treated as new.
 *
 * The raw `sql` mirror is excluded so that formatting, quoting and aliasing changes don't count as a change - the
 * compiled structure already normalizes those. Payload-query order is not significant, so those are sorted. Everything
 * else is compared verbatim, mirroring {@link serializedStreamBucketDataSourceEquality}. A behaviorally-neutral change
 * we don't normalize (e.g. reordering conjunction terms) simply reprocesses, which is the safe direction.
 */
export const serializedEventDefinitionEquality: Equality<SerializedEventDescriptor> = {
  hash(hasher, value) {
    hasher.addString(eventDefinitionIdentity(value));
  },
  equals(a, b) {
    return a === b || eventDefinitionIdentity(a) == eventDefinitionIdentity(b);
  }
};

function eventDefinitionIdentity(event: SerializedEventDescriptor): string {
  return JSON.stringify({
    name: event.name,
    sourceQueries: event.sourceQueries
      .map((query) => JSON.stringify({ table: query.table, variants: query.variants }))
      .sort()
  });
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
