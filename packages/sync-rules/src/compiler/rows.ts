import type { BucketDataSource, ParameterIndexLookupCreator } from '../BucketSource.js';
import { ImplicitSchemaTablePattern } from '../TablePattern.js';
import {
  EqualsIgnoringPrimaryResultSet,
  resultSetIgnoringEquality,
  TableValuedHashCodes,
  TableValuedIdentities
} from './compatibility.js';
import { StableHasher } from './equality.js';
import { RowExpression } from './filter.js';
import { PhysicalSourceResultSet, TableValuedResultSet } from './table.js';

/**
 * A key describing how buckets or parameter lookups are parameterized.
 *
 * When constructing buckets, a value needs to be passed for each such key.
 * When invoking parameter lookups, values need to be passed as inputs.
 */
export class PartitionKey implements EqualsIgnoringPrimaryResultSet {
  constructor(readonly expression: RowExpression) {}

  equalsAssumingSamePrimaryResultSet(
    other: EqualsIgnoringPrimaryResultSet,
    identities: TableValuedIdentities
  ): boolean {
    return (
      other instanceof PartitionKey && this.expression.equalsAssumingSamePrimaryResultSet(other.expression, identities)
    );
  }

  assumingSamePrimaryResultSetEqualityHashCode(codes: TableValuedHashCodes, hasher: StableHasher): void {
    this.expression.assumingSamePrimaryResultSetEqualityHashCode(codes, hasher);
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

  readonly #tableValuedFunctionToHashCode = new Map<TableValuedResultSet, number>();
  protected readonly tableValuedHashCodes: TableValuedHashCodes;

  constructor(options: SourceProcessorOptions) {
    this.syntacticSource = options.syntacticSource;
    this.filters = options.filters;
    this.partitionBy = options.partitionBy;
    this.addedFunctions = options.addedFunctions;

    this.tableValuedHashCodes = new TableValuedHashCodes(this.#tableValuedFunctionToHashCode);

    for (const fn of options.addedFunctions) {
      const hasher = new StableHasher();
      fn.assumingSamePrimaryResultSetEqualityHashCode(this.tableValuedHashCodes, hasher);
      this.#tableValuedFunctionToHashCode.set(fn.syntacticSource, hasher.buildHashCode());
    }
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
    const equality = resultSetIgnoringEquality(this.tableValuedHashCodes, TableValuedIdentities.empty);

    equality.unorderedEquality.hash(
      hasher,
      this.filters.map((f) => f.expression)
    );
    equality.listEquality.hash(hasher, this.partitionBy);
    equality.unorderedEquality.hash(hasher, this.addedFunctions);
  }

  protected baseMatchesOther(other: BaseSourceRowProcessor) {
    if (!other.tablePattern.equals(this.tablePattern)) {
      return null;
    }

    // Verify that table-valued functions on the two row processors are equal (in any order). Also, construct a mapping
    // of functions in both processors to a unique symbol so that if two table-valued functions are equal, the mapping
    // will report the exact same symbol. This allows expressions to compare references to table-valued functions across
    // the two processors.
    const symbolsForTableValuedFunctions = new Map<TableValuedResultSet, symbol>();
    const identities = new TableValuedIdentities(symbolsForTableValuedFunctions);

    {
      const unmatchedLocalFunctions = new Set(this.addedFunctions);

      other: for (const otherFunction of other.addedFunctions) {
        const otherHash = other.#tableValuedFunctionToHashCode.get(otherFunction.syntacticSource);

        for (const thisFunction of unmatchedLocalFunctions) {
          const thisHash = this.#tableValuedFunctionToHashCode.get(thisFunction.syntacticSource);

          if (thisHash === otherHash && thisFunction.equalsAssumingSamePrimaryResultSet(otherFunction, identities)) {
            const symbol = Symbol();
            symbolsForTableValuedFunctions.set(otherFunction.syntacticSource, symbol);
            symbolsForTableValuedFunctions.set(thisFunction.syntacticSource, symbol);
            unmatchedLocalFunctions.delete(thisFunction);

            continue other;
          }
        }

        // No match found for otherFunction
        return null;
      }
    }

    const equality = resultSetIgnoringEquality(TableValuedHashCodes.empty, identities);

    if (!equality.listEquality.equals(other.partitionBy, this.partitionBy)) {
      return null;
    }

    if (!equality.unorderedEquality.equals(other.filters, this.filters)) {
      return null;
    }

    return equality;
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
    this.tableValuedHashCodes.hashOrdered(this.columns, hasher);
    if (this.outputName) {
      hasher.addString(this.outputName);
    }
  }

  behavesIdenticalTo(other: RowEvaluator): boolean {
    if (other === this) return true;
    if (other.outputName != this.outputName) return false;

    const identities = this.baseMatchesOther(other);
    if (identities == null) return false;

    return identities.listEquality.equals(other.columns, this.columns);
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
    this.tableValuedHashCodes.hashOrdered(this.result, hasher);
  }

  behavesIdenticalTo(other: PointLookup): boolean {
    if (other === this) return true;

    const identities = this.baseMatchesOther(other);
    return identities != null && identities.listEquality.equals(other.result, this.result);
  }
}

/**
 * A table-valued function attached to a source processor.
 *
 * When processing source rows, all attached table-valued functions are expanded as well.
 */
export class SourceRowProcessorAddedTableValuedFunction implements EqualsIgnoringPrimaryResultSet {
  constructor(
    readonly syntacticSource: TableValuedResultSet,
    readonly functionName: string,
    readonly inputs: RowExpression[]
  ) {}

  equalsAssumingSamePrimaryResultSet(
    other: EqualsIgnoringPrimaryResultSet,
    identities: TableValuedIdentities
  ): boolean {
    if (!(other instanceof SourceRowProcessorAddedTableValuedFunction)) {
      return false;
    }

    return other.functionName == this.functionName && identities.orderedEquals(other.inputs, this.inputs);
  }

  assumingSamePrimaryResultSetEqualityHashCode(codes: TableValuedHashCodes, hasher: StableHasher): void {
    hasher.addString(this.functionName);
    codes.hashOrdered(this.inputs, hasher);
  }
}

export type ColumnSource = StarColumnSource | ExpressionColumnSource;

export class StarColumnSource implements EqualsIgnoringPrimaryResultSet {
  private constructor() {}

  equalsAssumingSamePrimaryResultSet(other: EqualsIgnoringPrimaryResultSet): boolean {
    return other instanceof StarColumnSource;
  }

  assumingSamePrimaryResultSetEqualityHashCode(): void {}

  static readonly instance = new StarColumnSource();
}

export class ExpressionColumnSource implements EqualsIgnoringPrimaryResultSet {
  constructor(
    readonly expression: RowExpression,
    readonly alias: string
  ) {}

  equalsAssumingSamePrimaryResultSet(
    other: EqualsIgnoringPrimaryResultSet,
    identities: TableValuedIdentities
  ): boolean {
    return (
      other instanceof ExpressionColumnSource &&
      other.alias == this.alias &&
      other.expression.equalsAssumingSamePrimaryResultSet(this.expression, identities)
    );
  }

  assumingSamePrimaryResultSetEqualityHashCode(codes: TableValuedHashCodes, hasher: StableHasher): void {
    this.expression.assumingSamePrimaryResultSetEqualityHashCode(codes, hasher);
    hasher.addString(this.alias);
  }
}
