import { HydrationInput } from '../BucketSource.js';
import { SourceTableRef } from '../SourceTableRef.js';
import { EvaluatedRowProjection, PendingRowProjection } from '../sync_plan/evaluator/row_projection.js';
import {
  CompiledEventDescriptor as CompiledEventDescriptorPlan,
  CompiledEventSourceQuery as CompiledEventSourceQueryPlan
} from '../sync_plan/plan.js';
import { TablePattern } from '../TablePattern.js';
import { EvaluateRowOptions, EvaluationError, SqliteRow } from '../types.js';
import {
  EvaluatedEventRowWithErrors,
  EvaluatedEventSourceRow,
  EventDefinition,
  HydratedEventDescriptor
} from './EventDescriptor.js';

/** A named event prepared from a compiled sync plan, before scalar expressions are prepared for evaluation. */
export class PreparedEventDefinition implements EventDefinition {
  readonly name: string;
  readonly sourceQueries: PreparedEventSourceQuery[];

  /**
   * Prepares each persisted payload query and resolves its source table against the sync config's default schema.
   */
  constructor(source: CompiledEventDescriptorPlan, defaultSchema: string) {
    this.name = source.name;
    this.sourceQueries = source.sourceQueries.map((query) => new PreparedEventSourceQuery(query, defaultSchema));
  }

  createEvaluator(input: HydrationInput): HydratedEventDescriptor {
    return new HydratedCompiledEventDescriptor(
      this.name,
      this.sourceQueries.map((query) => query.createEvaluator(input))
    );
  }

  getSourceTables(): Set<TablePattern> {
    return sourceTables(this.sourceQueries);
  }

  tableTriggersEvent(table: SourceTableRef): boolean {
    return this.sourceQueries.some((query) => query.applies(table));
  }
}

/** An event source query prepared from a compiled sync plan, before scalar expressions are evaluated. */
export class PreparedEventSourceQuery {
  readonly sourceTable: TablePattern;
  private readonly variants: PendingRowProjection[];

  /**
   * Resolves the source table and prepares each normalized query variant for later expression hydration.
   */
  constructor(source: CompiledEventSourceQueryPlan, defaultSchema: string) {
    this.sourceTable = source.sourceTable.toTablePattern(defaultSchema);
    this.variants = source.variants.map((variant) => new PendingRowProjection(variant, defaultSchema));
  }

  /**
   * Returns whether this payload query evaluates rows from the given physical source table.
   */
  applies(table: SourceTableRef): boolean {
    return this.sourceTable.matches(table);
  }

  /**
   * Instantiates the query's scalar expressions so its variants can evaluate replicated rows.
   */
  createEvaluator(input: HydrationInput): HydratedCompiledEventSourceQuery {
    return new HydratedCompiledEventSourceQuery(
      this.sourceTable,
      this.variants.map((variant) => variant.instantiate(input.scalarExpressions))
    );
  }
}

class HydratedCompiledEventDescriptor implements HydratedEventDescriptor {
  constructor(
    readonly name: string,
    readonly sourceQueries: HydratedCompiledEventSourceQuery[]
  ) {}

  evaluateRowWithErrors(options: EvaluateRowOptions): EvaluatedEventRowWithErrors {
    const matchingQueries = this.sourceQueries.filter((query) => query.applies(options.sourceTable));
    if (matchingQueries.length == 0) {
      return {
        results: [],
        errors: [{ error: `No matching source query found for table ${options.sourceTable.name}` }]
      };
    }

    const results: EvaluatedEventSourceRow[] = [];
    const errors: EvaluationError[] = [];
    for (const query of matchingQueries) {
      const evaluated = query.evaluateRowWithErrors(options.sourceTable, options.record);
      results.push(...evaluated.results);
      errors.push(...evaluated.errors);
    }

    return { results, errors };
  }

  getSourceTables(): Set<TablePattern> {
    return sourceTables(this.sourceQueries);
  }

  tableTriggersEvent(table: SourceTableRef): boolean {
    return this.sourceQueries.some((query) => query.applies(table));
  }
}

class HydratedCompiledEventSourceQuery {
  constructor(
    readonly sourceTable: TablePattern,
    private readonly variants: ((options: EvaluateRowOptions) => EvaluatedRowProjection[])[]
  ) {}

  /**
   * Returns whether this hydrated query evaluates rows from the given physical source table.
   */
  applies(table: SourceTableRef): boolean {
    return this.sourceTable.matches(table);
  }

  /**
   * Evaluates the normalized query variants as alternative matching branches, returning every payload produced by the
   * first matching branch and converting evaluation failures into event errors.
   */
  evaluateRowWithErrors(table: SourceTableRef, row: SqliteRow): EvaluatedEventRowWithErrors {
    try {
      for (const evaluate of this.variants) {
        const projected = evaluate({ sourceTable: table, record: row });
        if (projected.length > 0) {
          return { results: projected.map((result) => ({ data: result.data })), errors: [] };
        }
      }

      return { results: [], errors: [] };
    } catch (error) {
      return {
        results: [],
        errors: [{ error: error instanceof Error ? error.message : 'Evaluating event query failed' }]
      };
    }
  }
}

/**
 * Collects the physical source-table patterns referenced by a set of event payload queries.
 */
function sourceTables(queries: readonly { sourceTable: TablePattern }[]): Set<TablePattern> {
  return new Set(queries.map((query) => query.sourceTable));
}
