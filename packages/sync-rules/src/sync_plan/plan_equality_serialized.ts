import type { Equality } from '../compiler/equality.js';
import type {
  SerializedBucketDataSource,
  SerializedDataSource,
  SerializedEventDescriptor,
  SerializedEventRowEvaluator,
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
 * Equality for persisted replication events.
 *
 * Raw SQL is a rolling-upgrade compatibility mirror and cached evaluator hashes are not equality checks, so neither is
 * part of event behavior. Payload queries are unordered because runtime selects them by source table, and normalized
 * variants are unordered because they share a projection and only determine whether a row matches. The remaining
 * serialized plan is a stable, self-contained behavior representation: Expression ASTs include both their shape and
 * external-data bindings.
 */
export const serializedEventDefinitionEquality: Equality<SerializedEventDescriptor> = {
  hash(hasher, value) {
    hasher.addString(JSON.stringify(eventIdentity(value)));
  },
  equals(a, b) {
    return a === b || JSON.stringify(eventIdentity(a)) == JSON.stringify(eventIdentity(b));
  }
};

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

/**
 * Builds a normalized identity for comparing serialized event descriptors from persisted sync plans.
 *
 * Raw SQL is excluded, while source queries and variants are sorted because their order does not affect event
 * behavior.
 */
function eventIdentity(event: SerializedEventDescriptor) {
  return {
    name: event.name,
    sourceQueries: event.sourceQueries
      .map((query) =>
        JSON.stringify({
          table: query.table,
          variants: query.variants.map((variant) => JSON.stringify(eventRowEvaluatorIdentity(variant))).sort()
        })
      )
      .sort()
  };
}

/**
 * Selects the behavioral fields used when comparing row evaluators within serialized event descriptors.
 *
 * The cached evaluator hash is deliberately omitted. Ordering inside an evaluator is retained conservatively so
 * changes to projected column order or the compiler's filter-expression order invalidate persisted compatibility.
 */
function eventRowEvaluatorIdentity(evaluator: SerializedEventRowEvaluator) {
  return {
    table: evaluator.table,
    tableValuedFunctions: evaluator.tableValuedFunctions,
    filters: evaluator.filters,
    partitionBy: evaluator.partitionBy,
    columns: evaluator.columns
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
