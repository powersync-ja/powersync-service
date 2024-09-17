import { SqlRuleError } from '../errors.js';
import { IdSequence } from '../IdSequence.js';
import { SourceTableInterface } from '../SourceTableInterface.js';
import { QueryParseResult } from '../SqlBucketDescriptor.js';
import { TablePattern } from '../TablePattern.js';
import { EvaluateRowOptions, SourceSchema } from '../types.js';
import { EvaluatedEventRowWithErrors, SqlEventSourceQuery } from './SqlEventSourceQuery.js';

/**
 * A sync rules event which is triggered from a SQL table change.
 */
export class SqlEventDescriptor {
  name: string;
  source_queries: SqlEventSourceQuery[] = [];

  constructor(name: string, public idSequence: IdSequence) {
    this.name = name;
  }

  addSourceQuery(sql: string, schema?: SourceSchema): QueryParseResult {
    const source = SqlEventSourceQuery.fromSql(this.name, sql, schema);

    // Each source query should be for a unique table
    const existingSourceQuery = this.source_queries.find((q) => q.table == source.table);
    if (existingSourceQuery) {
      return {
        parsed: false,
        errors: [new SqlRuleError('Each payload query should query a unique table', sql)]
      };
    }

    source.ruleId = this.idSequence.nextId();
    this.source_queries.push(source);

    return {
      parsed: true,
      errors: source.errors
    };
  }

  evaluateRowWithErrors(options: EvaluateRowOptions): EvaluatedEventRowWithErrors {
    // There should only be 1 payload result per source query
    const matchingQuery = this.source_queries.find((q) => q.applies(options.sourceTable));
    if (!matchingQuery) {
      return {
        errors: [{ error: `No marching source query found for table ${options.sourceTable.table}` }]
      };
    }

    return matchingQuery.evaluateRowWithErrors(options.sourceTable, options.record);
  }

  getSourceTables(): Set<TablePattern> {
    let result = new Set<TablePattern>();
    for (let query of this.source_queries) {
      result.add(query.sourceTable!);
    }
    return result;
  }

  tableTriggersEvent(table: SourceTableInterface): boolean {
    return this.source_queries.some((query) => query.applies(table));
  }
}
