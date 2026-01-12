import { NodeLocation, parse, PGNode } from 'pgsql-ast-parser';
import { HashSet, StableHasher } from './equality.js';
import { PointLookup, RowEvaluator } from './rows.js';
import { StreamResolver } from './bucket_resolver.js';
import { StreamOptions, SyncPlan } from '../sync_plan/plan.js';
import { CompilerModelToSyncPlan } from './ir_to_sync_plan.js';
import { QuerierGraphBuilder } from './querier_graph.js';
import { StreamQueryParser } from './parser.js';

/**
 * State for compiling sync streams.
 */
export class SyncStreamsCompiler {
  readonly output = new CompiledStreamQueries();

  /**
   * Utility for compiling a sync stream.
   *
   * @param options Name, priority and `auto_subscribe` state for the stream.
   */
  stream(options: StreamOptions): IndividualSyncStreamCompiler {
    const builder = new QuerierGraphBuilder(this, options);

    return {
      addQuery: (sql: string, errors: ParsingErrorListener) => {
        const [stmt] = parse(sql, { locationTracking: true });
        const parser = new StreamQueryParser({
          originalText: sql,
          errors
        });
        const query = parser.parse(stmt);
        if (query) {
          builder.process(query, errors);
        }
      },
      finish: () => builder.finish()
    };
  }
}

/**
 * Utility for compiling a single sync stream.
 */
export interface IndividualSyncStreamCompiler {
  /**
   * Validates and adds a parameter query to this stream.
   *
   * @param sql The SQL query to add.
   * @param errors An error reporter associating source positions with the current SQL source.
   */
  addQuery(sql: string, errors: ParsingErrorListener): void;

  /**
   * Merges added queries into compatible bucket groups and adds them to the compiled sync plan.
   */
  finish(): void;
}

/**
 * Something reporting errors.
 *
 * While sync streams can be made up of multiple SQL statements from different YAML strings, we want to be able to
 * accurately describe the source of an error in YAML when we report it.
 *
 * So, every transformation that might need to report errors receives an instance of this interface which implicitly
 * binds errors to one specific SQL string.
 */
export interface ParsingErrorListener {
  report(message: string, location: NodeLocation | PGNode): void;
}

/**
 * A mutable collection of resources (row evaluators, point lookups and stream resolvers) created for all streams in a
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

  /**
   * @returns A sync plan representing an immutable snapshot of this intermediate representation.
   */
  toSyncPlan(): SyncPlan {
    const translator = new CompilerModelToSyncPlan();
    return translator.translate(this);
  }
}
