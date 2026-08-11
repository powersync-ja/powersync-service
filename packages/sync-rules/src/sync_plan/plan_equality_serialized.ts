import * as uuid from 'uuid';
import type { Equality } from '../compiler/equality.js';
import type { EventDefinitionId } from '../events/EventDescriptor.js';
import type {
  SerializedBucketDataSource,
  SerializedDataSource,
  SerializedEventDescriptor,
  SerializedEventRowEvaluator,
  SerializedEventSourceQuery,
  SerializedParameterIndexLookupCreator
} from './serialize.js';

const EVENT_DEFINITION_ID_NAMESPACE = uuid.v5('powersync-replication-event-definition-v1', uuid.v5.URL);

export interface SerializedBucketDataSourceWithDataSources {
  bucket: SerializedBucketDataSource;
  dataSources: readonly SerializedDataSource[];
}

export interface SerializedEventSourceDefinition {
  eventName: string;
  source: SerializedEventSourceQuery;
}

/** Returns the serialized event definition without its derived ID. */
export function serializedEventDefinitionIdentity(
  event: Pick<SerializedEventDescriptor, 'name' | 'sourceQueries'>
): string {
  return JSON.stringify({ name: event.name, sourceQueries: event.sourceQueries });
}

/** Generate the content-addressed ID persisted with and exposed by a compiled event definition. */
export function serializedEventDefinitionId(
  event: Pick<SerializedEventDescriptor, 'name' | 'sourceQueries'>
): EventDefinitionId {
  return uuid.v5(serializedEventDefinitionIdentity(event), EVENT_DEFINITION_ID_NAMESPACE);
}

/**
 * Compiled-plan equality for an individual event source query.
 *
 * Event source identity is independent of the containing sync config. The identity deliberately excludes raw SQL and
 * compiler hash codes, preserves table references as represented in the plan, and normalizes unordered filter variants
 * so callers can use it as stable input to a persisted fingerprint. Callers must still verify equality after a
 * fingerprint lookup.
 */
export const serializedEventSourceDefinitionEquality: Equality<SerializedEventSourceDefinition> = {
  hash(hasher, value) {
    hasher.addString(serializedEventSourceDefinitionIdentity(value));
  },
  equals(a, b) {
    return a === b || serializedEventSourceDefinitionIdentity(a) == serializedEventSourceDefinitionIdentity(b);
  }
};

/**
 * Returns the canonical, versioned identity input for one event source query.
 *
 * This is intentionally not a durable identifier by itself. Complete named events use
 * {@link serializedEventDefinitionId}; source-level callers can use this value for semantic comparisons.
 */
export function serializedEventSourceDefinitionIdentity(value: SerializedEventSourceDefinition): string {
  return JSON.stringify({
    version: 1,
    eventName: value.eventName,
    ...eventSourceQueryIdentity(value.source)
  });
}

/** Canonical identity fields for a source query without the containing event name. */
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
