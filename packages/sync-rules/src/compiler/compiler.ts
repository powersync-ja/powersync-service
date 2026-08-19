import { NodeLocation, parse, PGNode, Statement } from 'pgsql-ast-parser';
import { SqlRuleError } from '../errors.js';
import { CompiledEventDescriptor, StreamOptions, SyncPlan } from '../sync_plan/plan.js';
import { SourceSchema } from '../types.js';
import { StreamResolver } from './bucket_resolver.js';
import { DangerousParameterDetector } from './detect_dangerous_parameters.js';
import { HashSet } from './equality.js';
import { NodeLocations } from './expression.js';
import { RowExpression, SingleDependencyExpression } from './filter.js';
import { CompilerModelToSyncPlan } from './ir_to_sync_plan.js';
import { StreamQueryParser } from './parser.js';
import { QuerierGraphBuilder } from './querier_graph.js';
import { EventRowEvaluator, PointLookup, RowEvaluator } from './rows.js';
import { SqlScope } from './scope.js';
import { CommonTableExpression, PreparedSubquery } from './sqlite.js';
import { PhysicalSourceResultSet } from './table.js';

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

export interface ParseStreamOptions extends StreamOptions {
  warnOnDangerousParameter: boolean;
}

export interface CompiledEvent {
  name: string;
  sourceQueries: CompiledEventSourceQueryModel[];
}

export interface CompiledEventSourceQueryModel {
  sql: string;
  sourceTable: PhysicalSourceResultSet;
  variants: EventRowEvaluator[];
}

/**
 * State for compiling sync streams and replication events into a sync plan.
 *
 * The compiler stores a mutable intermediate representation that is essentially a copy of the resulting
 * {@link SyncPlan}, except that we're using JavaScript classes with methods to compute hash codes and equality
 * relations. Stream queries and event definitions remain separate within that model.
 *
 * The stream compilation process is as follows: Each data query for a stream is first parsed by
 * {@link StreamQueryParser} into a canonicalized intermediate representation (see that class for details).
 * Then, {@link QuerierGraphBuilder} analyzes a chain of `AND` expressions to identify parameters (as partition keys)
 * and their instantiation, as well as static filters that need to be added to reach row.
 */
export class SyncStreamsCompiler {
  readonly output: SyncPlanCompilerModel = {
    streams: new CompiledStreamQueries(),
    events: []
  };
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
  stream(options: ParseStreamOptions): IndividualSyncStreamCompiler {
    const builder = new QuerierGraphBuilder(this, {
      name: options.name,
      priority: options.priority,
      isSubscribedByDefault: options.isSubscribedByDefault
    });
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
      finish: () => {
        const buckets = builder.finish();
        if (options.warnOnDangerousParameter) {
          const detector = new DangerousParameterDetector();
          for (const bucket of buckets) {
            detector.processResolver(bucket);
          }
        }
      }
    };
  }

  /**
   * Compiles the payload queries for a named replication event.
   *
   * Event queries intentionally support a smaller surface than stream queries: They must project and filter a single
   * physical source table and cannot depend on request parameters, joins, subqueries or table-valued functions.
   */
  event(name: string): IndividualEventCompiler {
    const event: CompiledEvent = { name, sourceQueries: [] };
    this.output.events.push(event);

    return {
      addSourceQuery: (sql: string, errors: ParsingErrorListener) => {
        const stmt = tryParse(sql, errors);
        if (stmt == null) {
          return;
        }

        const parser = new StreamQueryParser({
          compiler: this,
          originalText: sql,
          locations: this.locations,
          parentScope: new SqlScope({}),
          errors
        });
        const query = parser.parse(stmt);
        if (query == null) {
          return;
        }

        if (query.joined.length != 0) {
          errors.report('Event payload queries must SELECT from a single physical source table.', query.span.location);
          return;
        }

        const defaultSchema = this.options.defaultSchema ?? '';
        const sourceTable = query.sourceTable.tablePattern.toTablePattern(defaultSchema);
        if (
          event.sourceQueries.some((source) =>
            source.sourceTable.tablePattern.toTablePattern(defaultSchema).equals(sourceTable)
          )
        ) {
          errors.report('Each payload query should query a unique table', query.span.location);
          return;
        }

        const variants: EventRowEvaluator[] = [];
        let valid = true;
        for (const variant of query.where.terms) {
          const filters: RowExpression[] = [];
          for (const term of variant.terms) {
            if (
              !(term instanceof SingleDependencyExpression) ||
              term.dependsOnConnection ||
              (term.resultSet != null && term.resultSet !== query.sourceTable)
            ) {
              errors.report(
                'Event payload queries cannot depend on request parameters or other tables.',
                term instanceof SingleDependencyExpression ? term.expression.location.location : term.location!
              );
              valid = false;
              continue;
            }

            filters.push(new RowExpression(term));
          }

          variants.push(
            new EventRowEvaluator({
              columns: query.resultColumns,
              syntacticSource: query.sourceTable,
              filters,
              partitionBy: [],
              addedFunctions: []
            })
          );
        }

        if (valid) {
          event.sourceQueries.push({ sql, sourceTable: query.sourceTable, variants });
        }
      }
    };
  }

  /**
   * @returns A sync plan representing an immutable snapshot of the compiler output.
   */
  toSyncPlan(): SyncPlan {
    const translator = new CompilerModelToSyncPlan();
    return translator.translate(this.output);
  }
}

/**
 * Compiles raw event SQL stored alongside older sync plans into the current plan representation.
 *
 * This is the compatibility boundary for plans written before compiled events were added. Callers should reject fatal
 * errors rather than carrying legacy event evaluators into a {@link SyncPlan}.
 */
export function compileEventDefinitions(
  definitions: Readonly<Record<string, readonly string[]>>,
  options: SyncStreamsCompilerOptions
): { events: CompiledEventDescriptor[]; errors: SqlRuleError[] } {
  const compiler = new SyncStreamsCompiler(options);
  const errors: SqlRuleError[] = [];

  for (const [name, queries] of Object.entries(definitions)) {
    const event = compiler.event(name);
    for (const sql of queries) {
      event.addSourceQuery(sql, {
        report(message, location, reportOptions) {
          const error = new SqlRuleError(message, sql, location);
          error.type = reportOptions?.isWarning ? 'warning' : 'fatal';
          errors.push(error);
        }
      });
    }
  }

  return { events: compiler.toSyncPlan().events, errors };
}

function tryParse(sql: string, errors: ParsingErrorListener): Statement | null {
  try {
    const statements = parse(sql, { locationTracking: true });
    if (statements.length != 1) {
      errors.report(
        'Only a single SELECT statement is supported',
        statements[1]?._location ?? { start: 0, end: sql.length }
      );
      return null;
    }

    const [stmt] = statements;
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
  registerCommonTableExpression(name: string, cte: CommonTableExpression): void;

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

export interface IndividualEventCompiler {
  /**
   * Validates and adds one payload query to this event.
   */
  addSourceQuery(sql: string, errors: ParsingErrorListener): void;
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
}

/**
 * Top-level compiler output used to assemble a complete sync plan.
 *
 * Streams and events are sibling sync-config concerns. Keeping their intermediate state separate prevents stream
 * compilation abstractions from acquiring event-specific responsibilities just because both are persisted in one
 * {@link SyncPlan}.
 */
export interface SyncPlanCompilerModel {
  readonly streams: CompiledStreamQueries;
  readonly events: CompiledEvent[];
}
