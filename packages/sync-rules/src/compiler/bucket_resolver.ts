import { Equatable, HashSet, StableHasher } from './equality.js';
import { equalsIgnoringResultSetList, equalsIgnoringResultSetUnordered } from './compatibility.js';
import { RequestExpression, RowExpression } from './filter.js';
import { PointLookup, RowEvaluator, SourceRowProcessor } from './rows.js';
import { RequestTableValuedResultSet } from './table.js';
import { StreamOptions } from '../sync_plan/plan.js';

export class StreamResolver {
  constructor(
    readonly options: StreamOptions,
    readonly requestFilters: RequestExpression[],
    readonly lookupStages: ExpandingLookup[][],
    readonly resolvedBucket: ResolveBucket,
    readonly uniqueName: string
  ) {}

  buildInstantiationHash(hasher: StableHasher) {
    equalsIgnoringResultSetUnordered.hash(hasher, this.requestFilters);
    this.resolvedBucket.buildInstantiationHash(hasher);
  }

  hasIdenticalInstantiation(other: StreamResolver) {
    if (other.options != this.options) {
      return false;
    }

    if (!equalsIgnoringResultSetUnordered.equals(this.requestFilters, this.requestFilters)) {
      return false;
    }

    return other.resolvedBucket.hasIdenticalInstantiation(this.resolvedBucket);
  }
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
  constructor(
    readonly tableValuedFunction: RequestTableValuedResultSet,
    readonly outputs: RowExpression[],
    readonly filters: RowExpression[]
  ) {}

  buildHash(hasher: StableHasher): void {
    this.tableValuedFunction.buildBehaviorHashCode(hasher);
    equalsIgnoringResultSetList.hash(hasher, this.outputs);
    equalsIgnoringResultSetList.hash(hasher, this.filters);
  }

  equals(other: unknown): boolean {
    return (
      other instanceof EvaluateTableValuedFunction &&
      other.tableValuedFunction.behavesIdenticalTo(this.tableValuedFunction) &&
      equalsIgnoringResultSetList.equals(other.outputs, this.outputs) &&
      equalsIgnoringResultSetList.equals(other.filters, this.filters)
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
    this.expression.assumingSameResultSetEqualityHashCode(hasher);
  }

  equals(other: unknown): boolean {
    return other instanceof RequestParameterValue && other.expression.equalsAssumingSameResultSet(this.expression);
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
