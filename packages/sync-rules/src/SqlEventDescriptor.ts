import { IdSequence } from './IdSequence.js';
import { SourceTableInterface } from './SourceTableInterface.js';
import { SqlParameterQuery } from './SqlParameterQuery.js';
import { TablePattern } from './TablePattern.js';
import { SqlRuleError } from './errors.js';
import { EvaluatedParametersResult, QueryParseOptions, SourceSchema, SqliteRow } from './types.js';

export interface QueryParseResult {
  /**
   * True if parsed in some form, even if there are errors.
   */
  parsed: boolean;

  errors: SqlRuleError[];
}

/**
 * A sync rules event which is triggered from a SQL table change.
 */
export class SqlEventDescriptor {
  name: string;
  bucket_parameters?: string[];

  constructor(name: string, public idSequence: IdSequence) {
    this.name = name;
  }

  parameter_queries: SqlParameterQuery[] = [];
  parameterIdSequence = new IdSequence();

  addParameterQuery(sql: string, schema: SourceSchema | undefined, options: QueryParseOptions): QueryParseResult {
    const parameterQuery = SqlParameterQuery.fromSql(this.name, sql, schema, options);
    parameterQuery.id = this.parameterIdSequence.nextId();
    if (false == parameterQuery instanceof SqlParameterQuery) {
      throw new Error('Parameter queries for events can not be global');
    }

    this.parameter_queries.push(parameterQuery as SqlParameterQuery);

    return {
      parsed: true,
      errors: parameterQuery.errors
    };
  }

  evaluateParameterRow(sourceTable: SourceTableInterface, row: SqliteRow): EvaluatedParametersResult[] {
    let results: EvaluatedParametersResult[] = [];
    for (let query of this.parameter_queries) {
      if (query.applies(sourceTable)) {
        results.push(...query.evaluateParameterRow(row));
      }
    }
    return results;
  }

  getSourceTables(): Set<TablePattern> {
    let result = new Set<TablePattern>();
    for (let query of this.parameter_queries) {
      result.add(query.sourceTable!);
    }
    return result;
  }

  tableTriggersEvent(table: SourceTableInterface): boolean {
    for (let query of this.parameter_queries) {
      if (query.applies(table)) {
        return true;
      }
    }
    return false;
  }
}
