import { NodeLocation, parse, PGNode, Statement } from 'pgsql-ast-parser';
import { HashSet } from './equality.js';
import { PointLookup, RowEvaluator } from './rows.js';
import { StreamResolver } from './bucket_resolver.js';
import { StreamOptions, SyncPlan } from '../sync_plan/plan.js';
import { CompilerModelToSyncPlan } from './ir_to_sync_plan.js';
import { QuerierGraphBuilder } from './querier_graph.js';
import { StreamQueryParser } from './parser.js';
import { NodeLocations } from './expression.js';
import { SqlScope } from './scope.js';
import { PreparedSubquery } from './sqlite.js';
import { SourceSchema } from '../types.js';

export interface SyncStreamsCompilerOptions {
  /**
   * Used exclusively for linting against the given {@link schema}.
   *
   * The default schema must not affect compiled sync plans because sync plans can be loaded with different default
   * schemas.
   */
  defaultSchema?: string;

  /**
   * An optional schema, used exclusively for linting table and column references that can't be resolved in it.
   *
   * Sync streams compile to the same plan regardless of the assumed schema, and it's possible to reuse compiled sync
   * streams across schema changes.
   */
  schema?: SourceSchema;
}

/**
 * State for compiling sync streams.
 *
 * The output of compiling all sync streams is a {@link SyncPlan}, a declarative description of the sync process that
 * can be serialized to bucket storage. The compiler stores a mutable intermediate representation that is essentially a
 * copy of the sync plan, except that we're using JavaScript classes with methods to compute hash codes and equality
 * relations. This allows the compiler to efficiently de-duplicate parameters and buckets.
 *
 * Overall, the compilation process is as follows: Each data query for a stream is first parsed by
 * {@link StreamQueryParser} into a canonicalized intermediate representation (see that class for details).
 * Then, {@link QuerierGraphBuilder} analyzes a chain of `AND` expressions to identify parameters (as partition keys)
 * and their instantiation, as well as static filters that need to be added to reach row.
 */
export class SyncStreamsCompiler {
  readonly output = new CompiledStreamQueries();
  private readonly locations = new NodeLocations();

  constructor(readonly options: SyncStreamsCompilerOptions) {}

  /**
   * Tries to parse the SQL query as a `SELECT` statement into a form supported for common table expressions.
   *
   * Common table expressions are parsed and validated independently and without a shared scope, meaning that CTEs are
   * not allowed to reference other CTEs. This limitation is deliberate, but we _could_ support it (referenced CTEs
   * would just get inlined into the new CTE by the parser). So we can revisit this and potentially support that in the
   * future.
   *
   * Returns null and reports errors if that fails.
   */
  commonTableExpression(sql: string, errors: ParsingErrorListener): PreparedSubquery | null {
    const parser = new StreamQueryParser({
      compiler: this,
      originalText: sql,
      locations: this.locations,
      parentScope: new SqlScope({}),
      errors
    });

    const stmt = tryParse(sql, errors);
    if (stmt == null) {
      return null;
    }
    return parser.parseAsSubquery(stmt);
  }

  /**
   * Utility for compiling a sync stream.
   *
   * @param options Name, priority and `auto_subscribe` state for the stream.
   */
  stream(options: StreamOptions): IndividualSyncStreamCompiler {
    const builder = new QuerierGraphBuilder(this, options);
    const rootScope = new SqlScope({});

    return {
      registerCommonTableExpression: (name, cte) => {
        rootScope.registerCommonTableExpression(name, cte);
      },
      addQuery: (sql: string, errors: ParsingErrorListener) => {
        const stmt = tryParse(sql, errors);
        if (stmt == null) {
          return;
        }
        const parser = new StreamQueryParser({
          compiler: this,
          originalText: sql,
          locations: this.locations,
          parentScope: rootScope,
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

function tryParse(sql: string, errors: ParsingErrorListener): Statement | null {
  try {
    const [stmt] = parse(sql, { locationTracking: true });
    return stmt;
  } catch (e: any) {
    const location: NodeLocation | undefined = e.token?._location;
    errors.report(e.message, location ?? { start: 0, end: sql.length });
    return null;
  }
}

/**
 * Utility for compiling a single sync stream.
 */
export interface IndividualSyncStreamCompiler {
  /**
   * Makes a common table expression prepared through {@link SyncStreamsCompiler.commonTableExpression} available when
   * parsing queries for this stream.
   */
  registerCommonTableExpression(name: string, cte: PreparedSubquery): void;

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
  report(message: string, location: NodeLocation | PGNode, options?: { isWarning: boolean }): void;
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
