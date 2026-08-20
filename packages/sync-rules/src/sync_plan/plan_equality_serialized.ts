import * as uuid from 'uuid';
import type { Equality } from '../compiler/equality.js';
import type { EventDefinitionId } from '../HydrationState.js';
import type {
  SerializedBucketDataSource,
  SerializedDataSource,
  SerializedEventDescriptorContent,
  SerializedEventRowEvaluator,
  SerializedEventSourceQuery,
  SerializedParameterIndexLookupCreator
} from './serialize.js';

const EVENT_DEFINITION_ID_NAMESPACE = uuid.v5('powersync-replication-event-definition-v1', uuid.v5.URL);

export interface SerializedBucketDataSourceWithDataSources {
  bucket: SerializedBucketDataSource;
  dataSources: readonly SerializedDataSource[];
}

/**
 * Returns the canonical, versioned identity input for a complete named event.
 *
 * Raw SQL and compiler hash codes are excluded because they do not define event behavior. Filter variants and source
 * queries are sorted because their order is not significant.
 *
 * This normalizes formatting and ordering, but not deeper SQL equivalences (e.g. commutative operands like
 * `a = b` vs `b = a`). Two behaviorally-identical definitions may therefore still produce different identities. That
 * only ever causes redundant reprocessing, never a missed change — which is the safe direction for incremental
 * reprocessing, and it still avoids reprocessing on the common formatting/ordering edits.
 */
export function serializedEventDefinitionIdentity(event: SerializedEventDescriptorContent): string {
  return JSON.stringify({
    version: 1,
    name: event.name,
    sourceQueries: event.sourceQueries.map((source) => JSON.stringify(eventSourceQueryIdentity(source))).sort()
  });
}

/**
 * Generate the content-addressed ID persisted with and exposed by a compiled event definition.
 *
 * We hash the canonical identity into a wide UUIDv5 rather than reusing the compiler's structural hash code, because
 * event ids are compared directly across sync configs (equality of persisted id strings) with no `equals()` fallback
 * to resolve collisions. The identifier must therefore be collision-resistant: a collision would make two different
 * events look identical and silently skip an event's reprocessing. The 32-bit structural hash is only safe where it is
 * paired with a full-equality check (e.g. bucket data sources).
 */
export function serializedEventDefinitionId(event: SerializedEventDescriptorContent): EventDefinitionId {
  return uuid.v5(serializedEventDefinitionIdentity(event), EVENT_DEFINITION_ID_NAMESPACE);
}

/** Canonical identity fields for a source query without raw SQL or compiler hashes. */
function eventSourceQueryIdentity(source: SerializedEventSourceQuery) {
  return {
    sourceTable: source.table,
    variants: source.variants
      .map(eventVariantIdentity)
      .map((variant) => JSON.stringify(variant))
      .sort()
  };
}

function eventVariantIdentity(value: SerializedEventRowEvaluator) {
  return {
    table: value.table,
    columns: value.columns.map((column) => {
      return column == 'star' ? column : { ...column, expr: canonicalExpression(column.expr) };
    }),
    // A conjunction's filter order does not affect behavior.
    filters: value.filters.map((filter) => JSON.stringify(canonicalExpression(filter))).sort(),
    tableValuedFunctions: value.tableValuedFunctions.map((fn) => ({
      ...fn,
      functionInputs: fn.functionInputs.map(canonicalExpression)
    })),
    partitionBy: value.partitionBy.map((key) => ({ expr: canonicalExpression(key.expr) }))
  };
}

function canonicalExpression(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(canonicalExpression);
  }
  if (value == null || typeof value != 'object') {
    return value;
  }

  const expression = value as Record<string, unknown>;
  if (expression.type == 'binary' && (expression.operator == 'and' || expression.operator == 'or')) {
    const operator = expression.operator;
    const operands: unknown[] = [];
    const addOperand = (operand: unknown) => {
      if (
        operand != null &&
        typeof operand == 'object' &&
        (operand as Record<string, unknown>).type == 'binary' &&
        (operand as Record<string, unknown>).operator == operator
      ) {
        addOperand((operand as Record<string, unknown>).left);
        addOperand((operand as Record<string, unknown>).right);
      } else {
        operands.push(canonicalExpression(operand));
      }
    };

    addOperand(expression.left);
    addOperand(expression.right);
    return { type: 'commutative', operator, operands: operands.map((operand) => JSON.stringify(operand)).sort() };
  }

  return Object.fromEntries(Object.entries(expression).map(([key, nested]) => [key, canonicalExpression(nested)]));
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
