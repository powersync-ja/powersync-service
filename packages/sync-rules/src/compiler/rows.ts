import type { ParameterIndexLookupCreator, BucketDataSource } from '../BucketSource.js';
import { StableHasher } from './equality.js';
import {
  EqualsIgnoringResultSet,
  equalsIgnoringResultSetList,
  equalsIgnoringResultSetUnordered
} from './compatibility.js';
import { RowExpression } from './filter.js';
import { PhysicalSourceResultSet, TableValuedResultSet } from './table.js';
import { ImplicitSchemaTablePattern } from '../TablePattern.js';

/**
 * A key describing how buckets or parameter lookups are parameterized.
 *
 * When constructing buckets, a value needs to be passed for each such key.
 * When invoking parameter lookups, values need to be passed as inputs.
 */
export abstract class PartitionKey implements EqualsIgnoringResultSet {
  abstract equalsAssumingSameResultSet(other: EqualsIgnoringResultSet): boolean;
  abstract assumingSameResultSetEqualityHashCode(hasher: StableHasher): void;
}

export class ScalarPartitionKey extends PartitionKey {
  constructor(readonly expression: RowExpression) {
    super();
  }

  equalsAssumingSameResultSet(other: EqualsIgnoringResultSet): boolean {
    return (
      other instanceof ScalarPartitionKey &&
      other.expression.expression.equalsAssumingSameResultSet(this.expression.expression)
    );
  }

  assumingSameResultSetEqualityHashCode(hasher: StableHasher): void {
    this.expression.expression.assumingSameResultSetEqualityHashCode(hasher);
  }
}

/**
 * A partition key derived from the output of a table-valued function added to a {@link SourceRowProcessor}.
 */
export class TableValuedPartitionKey extends PartitionKey {
  constructor(
    readonly fromFunction: SourceRowProcessorAddedTableValuedFunction,
    readonly output: RowExpression
  ) {
    super();
  }

  equalsAssumingSameResultSet(other: EqualsIgnoringResultSet): boolean {
    if (!(other instanceof TableValuedPartitionKey)) {
      return false;
    }

    return (
      other.fromFunction.equalsAssumingSameResultSet(this.fromFunction) &&
      other.output.equalsAssumingSameResultSet(this.output)
    );
  }

  assumingSameResultSetEqualityHashCode(hasher: StableHasher): void {
    this.fromFunction.assumingSameResultSetEqualityHashCode(hasher);
    this.output.assumingSameResultSetEqualityHashCode(hasher);
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
  readonly addedFunctions: SourceRowProcessorAddedTableValuedFunction[];
}

abstract class BaseSourceRowProcessor {
  /**
   * The table names being matched, along with an AST reference describing its syntactic source.
   */
  readonly syntacticSource: PhysicalSourceResultSet;

  /**
   * Filters which all depend on {@link syntacticSource} exclusively.
   *
   * This processor is only active on rows matching these filters.
   */
  readonly filters: RowExpression[];
  readonly partitionBy: PartitionKey[];
  readonly addedFunctions: SourceRowProcessorAddedTableValuedFunction[];

  constructor(options: SourceProcessorOptions) {
    this.syntacticSource = options.syntacticSource;
    this.filters = options.filters;
    this.partitionBy = options.partitionBy;
    this.addedFunctions = options.addedFunctions;
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
  get tablePattern(): ImplicitSchemaTablePattern {
    return this.syntacticSource.tablePattern;
  }

  protected addBaseHashCode(hasher: StableHasher) {
    hasher.add(this.tablePattern);
    equalsIgnoringResultSetUnordered.hash(
      hasher,
      this.filters.map((f) => f.expression)
    );
    equalsIgnoringResultSetList.hash(hasher, this.partitionBy);
    equalsIgnoringResultSetUnordered.hash(hasher, this.addedFunctions);
  }

  protected baseMatchesOther(other: BaseSourceRowProcessor) {
    if (!other.tablePattern.equals(this.tablePattern)) {
      return false;
    }

    if (!equalsIgnoringResultSetList.equals(other.partitionBy, this.partitionBy)) {
      return false;
    }

    if (!equalsIgnoringResultSetUnordered.equals(other.filters, this.filters)) {
      return false;
    }

    if (!equalsIgnoringResultSetUnordered.equals(other.addedFunctions, this.addedFunctions)) {
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

  get outputName(): string | undefined {
    const alias = this.syntacticSource.source.explicitName;

    if (this.syntacticSource.tablePattern.isWildcard) {
      if (alias == null) {
        // Unaliased wildcard, use source table name.
        return undefined;
      }
    }

    return alias ?? this.syntacticSource.tablePattern.tablePattern;
  }

  buildBehaviorHashCode(hasher: StableHasher): void {
    this.addBaseHashCode(hasher);
    equalsIgnoringResultSetList.hash(hasher, this.columns);
    if (this.outputName) {
      hasher.addString(this.outputName);
    }
  }

  behavesIdenticalTo(other: RowEvaluator): boolean {
    return (
      this.baseMatchesOther(other) &&
      equalsIgnoringResultSetList.equals(other.columns, this.columns) &&
      other.outputName == this.outputName
    );
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

/**
 * A table-valued function attached to a source processor.
 *
 * When processing source rows, all attached table-valued functions are expanded as well.
 */
export class SourceRowProcessorAddedTableValuedFunction implements EqualsIgnoringResultSet {
  constructor(
    readonly syntacticSource: TableValuedResultSet,
    readonly functionName: string,
    readonly inputs: RowExpression[],
    readonly filters: RowExpression[]
  ) {}

  equalsAssumingSameResultSet(other: EqualsIgnoringResultSet): boolean {
    if (!(other instanceof SourceRowProcessorAddedTableValuedFunction)) {
      return false;
    }

    return (
      other.functionName == this.functionName &&
      equalsIgnoringResultSetList.equals(other.inputs, this.inputs) &&
      equalsIgnoringResultSetList.equals(other.filters, this.filters)
    );
  }

  assumingSameResultSetEqualityHashCode(hasher: StableHasher): void {
    hasher.addString(this.functionName);
    equalsIgnoringResultSetList.hash(hasher, this.inputs);
    equalsIgnoringResultSetUnordered.hash(hasher, this.filters);
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
    readonly alias: string
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
    hasher.addString(this.alias);
  }
}
