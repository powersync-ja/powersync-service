import { StreamOptions } from '../sync_plan/plan.js';
import { TableValuedFunctionEquality } from './compatibility.js';
import { Equality, Equatable, HashSet, StableHasher, unorderedEquality } from './equality.js';
import { RequestExpression, RowExpression } from './filter.js';
import { PointLookup, RowEvaluator, SourceRowProcessor } from './rows.js';
import { TableValuedResultSet } from './table.js';

/**
 * Describes how to resolve a subscription to buckets.
 */
export class StreamResolver {
  constructor(
    readonly options: StreamOptions,
    readonly requestFilters: RequestExpression[],
    readonly lookupStages: ExpandingLookup[][],
    readonly resolvedBucket: ResolveBucket,
    readonly uniqueName: string
  ) {}

  buildInstantiationHash(hasher: StableHasher) {
    TableValuedFunctionEquality.empty.unorderedEquality.hash(hasher, this.requestFilters);
    StreamResolver.lookupStageEquality.hash(hasher, this.lookupStages);
    this.resolvedBucket.buildInstantiationHash(hasher);
  }

  hasIdenticalInstantiation(other: StreamResolver) {
    if (other.options != this.options) {
      return false;
    }

    if (!TableValuedFunctionEquality.empty.unorderedEquality.equals(other.requestFilters, this.requestFilters)) {
      return false;
    }

    if (!StreamResolver.lookupStageEquality.equals(other.lookupStages, this.lookupStages)) {
      return false;
    }

    return other.resolvedBucket.hasIdenticalInstantiation(this.resolvedBucket);
  }

  // When comparing lookup stages, we don't care about the order and how lookups have been assigned into stages.
  // Each inner lookup would include its input in its equality/hashcode implementation, so we get the ordering through
  // that. And as long as that input structure matches, two resolvers with the same lookups in a different order are
  // still equal.
  private static readonly flatLookupEquality = unorderedEquality(StableHasher.defaultEquality);

  private static readonly lookupStageEquality: Equality<ExpandingLookup[][]> = {
    equals: function (a: ExpandingLookup[][], b: ExpandingLookup[][]): boolean {
      return StreamResolver.flatLookupEquality.equals(
        a.flatMap((s) => s),
        b.flatMap((s) => s)
      );
    },
    hash: function (hasher: StableHasher, value: ExpandingLookup[][]): void {
      return StreamResolver.flatLookupEquality.hash(
        hasher,
        value.flatMap((s) => s)
      );
    }
  };
}

/**
 * A lookup returning multiple rows when instantiated.
 */
export type ExpandingLookup = ParameterLookup | EvaluateTableValuedFunction;

export class ParameterLookup implements Equatable {
  constructor(
    readonly lookup: PointLookup,
    readonly instantiation: ParameterValue[]
  ) {}

  buildHash(hasher: StableHasher): void {
    hasher.addHash(this.lookup.behaviorHashCode);
    hasher.add(...this.instantiation);
  }

  equals(other: unknown): boolean {
    return (
      other instanceof ParameterLookup &&
      other.lookup.behavesIdenticalTo(this.lookup) &&
      StableHasher.defaultListEquality.equals(other.instantiation, this.instantiation)
    );
  }
}

export class EvaluateTableValuedFunction implements Equatable {
  readonly #hashes: TableValuedFunctionEquality;

  constructor(
    readonly tableValuedFunction: TableValuedResultSet,
    readonly outputs: RowExpression[],
    readonly filters: RowExpression[]
  ) {
    this.#hashes = TableValuedFunctionEquality.forSingleTableValuedFunction(tableValuedFunction);
  }

  buildHash(hasher: StableHasher): void {
    this.tableValuedFunction.buildBehaviorHashCode(TableValuedFunctionEquality.empty, hasher);
    this.#hashes.listEquality.hash(hasher, this.outputs);
    this.#hashes.unorderedEquality.hash(hasher, this.filters);
  }

  equals(other: unknown): boolean {
    if (
      !(other instanceof EvaluateTableValuedFunction) ||
      !other.tableValuedFunction.behavesIdenticalTo(this.tableValuedFunction, TableValuedFunctionEquality.empty)
    ) {
      return false;
    }

    const symbolMap = new Map<TableValuedResultSet, symbol>();
    const symbol = Symbol();
    symbolMap.set(this.tableValuedFunction, symbol);
    symbolMap.set(other.tableValuedFunction, symbol);

    const equality = TableValuedFunctionEquality.mergeForEquality(this.#hashes, other.#hashes, symbolMap);

    return (
      equality.listEquality.equals(this.outputs, other.outputs) &&
      equality.unorderedEquality.equals(this.filters, other.filters)
    );
  }
}

export class ResolveBucket {
  readonly evaluators = new HashSet<RowEvaluator>({
    hash: (hasher, value) => value.buildBehaviorHashCode(hasher),
    equals: (a, b) => a.behavesIdenticalTo(b)
  });

  constructor(
    evaluator: RowEvaluator,
    readonly instantiation: ParameterValue[]
  ) {
    this.evaluators.add(evaluator);
  }

  buildInstantiationHash(hasher: StableHasher) {
    hasher.add(...this.instantiation);
  }

  hasIdenticalInstantiation(other: ResolveBucket) {
    return StableHasher.defaultListEquality.equals(other.instantiation, this.instantiation);
  }
}

/**
 * A value passed as input to a partition key of a {@link SourceRowProcessor}
 */
export type ParameterValue = RequestParameterValue | LookupResultParameterValue | IntersectionParameterValue;

/**
 * A value derived from request data.
 */
export class RequestParameterValue implements Equatable {
  constructor(readonly expression: RequestExpression) {}

  buildHash(hasher: StableHasher): void {
    this.expression.assumingSamePrimaryResultSetEqualityHashCode(TableValuedFunctionEquality.empty, hasher);
  }

  equals(other: unknown): boolean {
    return (
      other instanceof RequestParameterValue &&
      this.expression.equalsAssumingSamePrimaryResultSet(other.expression, TableValuedFunctionEquality.empty)
    );
  }
}

export class LookupResultParameterValue implements Equatable {
  lookup: ExpandingLookup | undefined; // Set lazily

  constructor(readonly resultIndex: number) {}

  buildHash(hasher: StableHasher): void {
    this.lookup?.buildHash(hasher);
    hasher.addHash(this.resultIndex);
  }

  equals(other: ParameterValue): boolean {
    return (
      other instanceof LookupResultParameterValue &&
      other.lookup!.equals(this.lookup) &&
      other.resultIndex == this.resultIndex
    );
  }
}

export class IntersectionParameterValue implements Equatable {
  constructor(readonly inner: ParameterValue[]) {}

  buildHash(hasher: StableHasher): void {
    hasher.add(...this.inner);
  }

  equals(other: unknown): boolean {
    return (
      other instanceof IntersectionParameterValue && StableHasher.defaultListEquality.equals(other.inner, this.inner)
    );
  }
}
