import { HydrationInput } from '../BucketSource.js';
import { SourceTableRef } from '../SourceTableRef.js';
import { TablePattern } from '../TablePattern.js';
import { EvaluateRowOptions, EvaluationError, SqliteJsonRow } from '../types.js';

export type EvaluatedEventSourceRow = {
  data: SqliteJsonRow;
};

export type EvaluatedEventRowWithErrors = {
  result?: EvaluatedEventSourceRow;
  errors: EvaluationError[];
};

/** A parsed event definition whose compiled expressions have not yet been prepared for evaluation. */
export interface EventDefinition {
  readonly name: string;

  createEvaluator(input: HydrationInput): HydratedEventDescriptor;
  getSourceTables(): Set<TablePattern>;
  tableTriggersEvent(table: SourceTableRef): boolean;
}

/** An event definition whose payload queries can evaluate replicated rows. */
export interface HydratedEventDescriptor {
  readonly name: string;

  evaluateRowWithErrors(options: EvaluateRowOptions): EvaluatedEventRowWithErrors;
  getSourceTables(): Set<TablePattern>;
  tableTriggersEvent(table: SourceTableRef): boolean;
}
