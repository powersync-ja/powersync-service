import { NodeLocation, PGNode } from 'pgsql-ast-parser';
import { HashSet } from './equality.js';
import { PointLookup, RowEvaluator } from '../ir/rows.js';
import { StreamResolver } from '../ir/bucket_resolver.js';

export interface ParsingErrorListener {
  report(message: string, location: NodeLocation | PGNode): void;
}

export class SyncStreamCompiler {
  readonly output = new CompiledStreamQueries();
}

/**
 * A collection of resources (row evaluators, point lookups and stream resolvers) created for all streams in a
 * definition file.
 */
export class CompiledStreamQueries {
  private readonly _evaluators = new HashSet<RowEvaluator>({
    hash: (hasher, value) => value.buildBehaviorHashCode(hasher),
    equals: (a, b) => a.behavesIdenticalTo(b)
  });
  private readonly _pointLookups = new HashSet<PointLookup>({
    hash: (hasher, value) => value.buildBehaviorHashCode(hasher),
    equals: (a, b) => a.behavesIdenticalTo(b)
  });

  readonly resolvers: StreamResolver[] = [];

  get evaluators(): RowEvaluator[] {
    return [...this._evaluators];
  }

  get pointLookups(): PointLookup[] {
    return [...this._pointLookups];
  }

  canonicalizeEvaluator(evaluator: RowEvaluator): RowEvaluator {
    return this._evaluators.getOrInsert(evaluator)[0];
  }

  canonicalizePointLookup(lookup: PointLookup): PointLookup {
    return this._pointLookups.getOrInsert(lookup)[0];
  }
}
