import type { ParameterIndexLookupCreator, BucketDataSource } from '../BucketSource.js';
import { StableHasher } from './equality.js';
import {
  equalsIgnoringResultSet,
  EqualsIgnoringResultSet,
  equalsIgnoringResultSetList,
  equalsIgnoringResultSetUnordered
} from './compatibility.js';
import { RowExpression } from './filter.js';
import { PhysicalSourceResultSet } from './table.js';

/**
 * A key describing how buckets or parameter lookups are parameterized.
 *
 * When constructing buckets, a value needs to be passed for each such key.
 * WHen invoking parameter lookups, values need to be passed as inputs.
 */
export class PartitionKey implements EqualsIgnoringResultSet {
  constructor(readonly expression: RowExpression) {}

  equalsAssumingSameResultSet(other: EqualsIgnoringResultSet): boolean {
    return (
      other instanceof PartitionKey &&
      other.expression.expression.equalsAssumingSameResultSet(this.expression.expression)
    );
  }

  assumingSameResultSetEqualityHashCode(hasher: StableHasher): void {
    this.expression.expression.assumingSameResultSetEqualityHashCode(hasher);
  }
}

/**
 * Something that processes source rows during replication.
 *
 * This includes {@link RowEvaluator}s, which assigns rows into buckets, and {@link PointLookup}, which creates
 * parameter lookups used to resolve bucket ids when a user connects.
 */
export type SourceRowProcessor = RowEvaluator | PointLookup;

interface SourceProcessorOptions {
  readonly syntacticSource: PhysicalSourceResultSet;
  readonly filters: RowExpression[];
  readonly partitionBy: PartitionKey[];
}

abstract class BaseSourceRowProcessor {
  /**
   * The table names being matched, along with an AST reference describing its syntactic source.
   */
  readonly syntacticSource: PhysicalSourceResultSet;

  /**
   * Filters which all depend on {@link syntacticSource} exclusively.
   *
   * This processor is only active on rows
   */
  readonly filters: RowExpression[];
  readonly partitionBy: PartitionKey[];

  constructor(options: SourceProcessorOptions) {
    this.syntacticSource = options.syntacticSource;
    this.filters = options.filters;
    this.partitionBy = options.partitionBy;
    options.partitionBy.sort(
      (a, b) => StableHasher.hashWith(equalsIgnoringResultSet, a) - StableHasher.hashWith(equalsIgnoringResultSet, b)
    );
  }

  /**
   * A hash code for the equivalence relation formed by {@link behavesIdenticalTo}.
   */
  abstract buildBehaviorHashCode(hasher: StableHasher): void;

  get behaviorHashCode(): number {
    const hasher = new StableHasher();
    this.buildBehaviorHashCode(hasher);
    return hasher.buildHashCode();
  }

  /**
   * Whether two source row processors behave identically.
   *
   * If this is the case, they can be re-used across different stream definitions or even different sync rule instances
   * (for incremental reprocessing).
   */
  abstract behavesIdenticalTo(other: this): boolean;

  /**
   * The table pattern matched by this bucket or parameter lookup creator.
   */
  get tablePattern(): string {
    return this.syntacticSource.tablePattern;
  }

  protected addBaseHashCode(hasher: StableHasher) {
    hasher.addString(this.tablePattern);
    equalsIgnoringResultSetUnordered.hash(
      hasher,
      this.filters.map((f) => f.expression)
    );
    equalsIgnoringResultSetList.hash(hasher, this.partitionBy);
  }

  protected baseMatchesOther(other: BaseSourceRowProcessor) {
    if (other.tablePattern != this.tablePattern) {
      return false;
    }

    if (!equalsIgnoringResultSetList.equals(other.partitionBy, this.partitionBy)) {
      return false;
    }

    if (!equalsIgnoringResultSetUnordered.equals(other.filters, this.filters)) {
      return false;
    }

    return true;
  }
}

/**
 * A row evaluator, evaluating rows to sync from a row in the source database.
 *
 * During replication, instances of these are implemented as {@link BucketDataSource}.
 */
export class RowEvaluator extends BaseSourceRowProcessor {
  /**
   * Expressions and names for columns to sync.
   */
  readonly columns: ColumnSource[];

  constructor(options: SourceProcessorOptions & { columns: ColumnSource[] }) {
    super(options);
    this.columns = options.columns;
  }

  buildBehaviorHashCode(hasher: StableHasher): void {
    this.addBaseHashCode(hasher);
    equalsIgnoringResultSetList.hash(hasher, this.columns);
  }

  behavesIdenticalTo(other: RowEvaluator): boolean {
    return this.baseMatchesOther(other) && equalsIgnoringResultSetList.equals(other.columns, this.columns);
  }
}

/**
 * A point lookup, creating a materialized index.
 *
 * These are used to implement subqueries. E.g for `SELECT * FROM users WHERE org IN (SELECT id FROM orgs WHERE name =
 * auth.param('org'))`, we would create a point lookup on `orgs` with a partition key of `name` and a result including
 * `id`.
 *
 * During replication, instances of these are implemented as {@link ParameterIndexLookupCreator}s.
 */
export class PointLookup extends BaseSourceRowProcessor {
  /**
   * Outputs of the point lookup, which can be used when querying for buckets.
   */
  readonly result: RowExpression[];

  constructor(options: SourceProcessorOptions & { result: RowExpression[] }) {
    super(options);
    this.result = options.result;
  }

  buildBehaviorHashCode(hasher: StableHasher): void {
    this.addBaseHashCode(hasher);
    equalsIgnoringResultSetList.hash(hasher, this.result);
  }

  behavesIdenticalTo(other: PointLookup): boolean {
    return this.baseMatchesOther(other) && equalsIgnoringResultSetList.equals(other.result, this.result);
  }
}

export type ColumnSource = StarColumnSource | ExpressionColumnSource;

export class StarColumnSource implements EqualsIgnoringResultSet {
  private constructor() {}

  equalsAssumingSameResultSet(other: EqualsIgnoringResultSet): boolean {
    return other instanceof StarColumnSource;
  }

  assumingSameResultSetEqualityHashCode(): void {}

  static readonly instance = new StarColumnSource();
}

export class ExpressionColumnSource implements EqualsIgnoringResultSet {
  constructor(
    readonly expression: RowExpression,
    readonly alias?: string
  ) {}

  equalsAssumingSameResultSet(other: EqualsIgnoringResultSet): boolean {
    return (
      other instanceof ExpressionColumnSource &&
      other.alias == this.alias &&
      other.expression.equalsAssumingSameResultSet(this.expression)
    );
  }

  assumingSameResultSetEqualityHashCode(hasher: StableHasher): void {
    this.expression.assumingSameResultSetEqualityHashCode(hasher);

    if (this.alias != null) {
      hasher.addString(this.alias);
    }
  }
}
