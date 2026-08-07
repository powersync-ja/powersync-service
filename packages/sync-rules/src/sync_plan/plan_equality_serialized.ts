import { Equality } from '../compiler/equality.js';
import { DEFAULT_TAG } from '../TablePattern.js';
import {
  SerializedBucketDataSource,
  SerializedDataSource,
  SerializedEventRowEvaluator,
  SerializedEventSourceQuery,
  SerializedParameterIndexLookupCreator
} from './serialize.js';

export interface SerializedBucketDataSourceWithDataSources {
  bucket: SerializedBucketDataSource;
  dataSources: readonly SerializedDataSource[];
}

export interface SerializedEventSourceDefinition {
  eventName: string;
  defaultSchema: string;
  source: SerializedEventSourceQuery;
}

/**
 * Semantic equality for an individual event source query.
 *
 * Event source identity is independent of the containing sync config. The identity deliberately excludes raw SQL and
 * compiler hash codes, and normalizes unordered filter variants so callers can use it as stable input to a persisted
 * fingerprint. Callers must still verify equality after a fingerprint lookup.
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
 * This is intentionally not a durable identifier by itself. Storage implementations can hash the returned value for
 * lookup and then use {@link serializedEventSourceDefinitionEquality} to guard against collisions.
 */
export function serializedEventSourceDefinitionIdentity(value: SerializedEventSourceDefinition): string {
  const variants = value.source.variants
    .map((variant) => eventVariantIdentity(variant, value.defaultSchema))
    .map((variant) => JSON.stringify(variant))
    .sort();

  return JSON.stringify({
    version: 1,
    eventName: value.eventName,
    sourceTable: resolvedTableIdentity(value.source.table, value.defaultSchema),
    variants
  });
}

function eventVariantIdentity(value: SerializedEventRowEvaluator, defaultSchema: string) {
  return {
    table: resolvedTableIdentity(value.table, defaultSchema),
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

function resolvedTableIdentity(table: SerializedEventRowEvaluator['table'], defaultSchema: string) {
  return { ...table, connection: table.connection ?? DEFAULT_TAG, schema: table.schema ?? defaultSchema };
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
