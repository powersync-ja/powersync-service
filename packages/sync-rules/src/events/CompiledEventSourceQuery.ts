import { HydrationInput } from '../BucketSource.js';
import { EventDefinitionId } from '../HydrationState.js';
import { SourceTableRef } from '../SourceTableRef.js';
import { EvaluatedRowProjection, PendingRowProjection } from '../sync_plan/evaluator/row_projection.js';
import {
  CompiledEventDescriptor as CompiledEventDescriptorPlan,
  CompiledEventSourceQuery as CompiledEventSourceQueryPlan
} from '../sync_plan/plan.js';
import { TablePattern } from '../TablePattern.js';
import { EvaluateRowOptions, SqliteRow } from '../types.js';
import { EvaluatedEventRowWithErrors, EventDefinition, HydratedEventDescriptor } from './EventDescriptor.js';

/** A named event prepared from a compiled sync plan, before scalar expressions are prepared for evaluation. */
export class PreparedEventDefinition implements EventDefinition {
  readonly id: EventDefinitionId;
  readonly name: string;
  readonly sourceQueries: PreparedEventSourceQuery[];

  constructor(source: CompiledEventDescriptorPlan, defaultSchema: string) {
    this.id = source.id;
    this.name = source.name;
    this.sourceQueries = source.sourceQueries.map((query) => new PreparedEventSourceQuery(query, defaultSchema));
  }

  createEvaluator(input: HydrationInput): HydratedEventDescriptor {
    return new HydratedCompiledEventDescriptor(
      this.id,
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

  constructor(source: CompiledEventSourceQueryPlan, defaultSchema: string) {
    this.sourceTable = source.sourceTable.toTablePattern(defaultSchema);
    this.variants = source.variants.map((variant) => new PendingRowProjection(variant, defaultSchema));
  }

  applies(table: SourceTableRef): boolean {
    return this.sourceTable.matches(table);
  }

  createEvaluator(input: HydrationInput): HydratedCompiledEventSourceQuery {
    return new HydratedCompiledEventSourceQuery(
      this.sourceTable,
      this.variants.map((variant) => variant.instantiate(input.scalarExpressions))
    );
  }
}

class HydratedCompiledEventDescriptor implements HydratedEventDescriptor {
  constructor(
    readonly id: EventDefinitionId,
    readonly name: string,
    readonly sourceQueries: HydratedCompiledEventSourceQuery[]
  ) {}

  evaluateRowWithErrors(options: EvaluateRowOptions): EvaluatedEventRowWithErrors {
    const matchingQuery = this.sourceQueries.find((query) => query.applies(options.sourceTable));
    if (matchingQuery == null) {
      return { errors: [{ error: `No matching source query found for table ${options.sourceTable.name}` }] };
    }

    return matchingQuery.evaluateRowWithErrors(options.sourceTable, options.record);
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

  applies(table: SourceTableRef): boolean {
    return this.sourceTable.matches(table);
  }

  evaluateRowWithErrors(table: SourceTableRef, row: SqliteRow): EvaluatedEventRowWithErrors {
    try {
      for (const evaluate of this.variants) {
        const [result] = evaluate({ sourceTable: table, record: row });
        if (result != null) {
          return { result: { data: result.data }, errors: [] };
        }
      }

      return { errors: [] };
    } catch (error) {
      return { errors: [{ error: error instanceof Error ? error.message : 'Evaluating event query failed' }] };
    }
  }
}

function sourceTables(queries: readonly { sourceTable: TablePattern }[]): Set<TablePattern> {
  return new Set(queries.map((query) => query.sourceTable));
}
