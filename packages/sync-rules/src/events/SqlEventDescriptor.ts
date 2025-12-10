import { SqlRuleError } from '../errors.js';
import { CompatibilityContext } from '../compatibility.js';
import { SourceTableInterface } from '../SourceTableInterface.js';
import { QueryParseResult } from '../SqlBucketDescriptor.js';
import { SyncRulesOptions } from '../SqlSyncRules.js';
import { TablePattern } from '../TablePattern.js';
import { EvaluateRowOptions } from '../types.js';
import { EvaluatedEventRowWithErrors, SqlEventSourceQuery } from './SqlEventSourceQuery.js';

/**
 * A sync rules event which is triggered from a SQL table change.
 */
export class SqlEventDescriptor {
  name: string;
  sourceQueries: SqlEventSourceQuery[] = [];

  constructor(
    name: string,
    private readonly compatibility: CompatibilityContext
  ) {
    this.name = name;
  }

  addSourceQuery(sql: string, options: SyncRulesOptions): QueryParseResult {
    const source = SqlEventSourceQuery.fromSql(sql, options, this.compatibility);

    // Each source query should be for a unique table
    const existingSourceQuery = this.sourceQueries.find((q) => q.table == source.table);
    if (existingSourceQuery) {
      return {
        parsed: false,
        errors: [new SqlRuleError('Each payload query should query a unique table', sql)]
      };
    }

    this.sourceQueries.push(source);

    return {
      parsed: true,
      errors: source.errors
    };
  }

  evaluateRowWithErrors(options: EvaluateRowOptions): EvaluatedEventRowWithErrors {
    // There should only be 1 payload result per source query
    const matchingQuery = this.sourceQueries.find((q) => q.applies(options.sourceTable));
    if (!matchingQuery) {
      return {
        errors: [{ error: `No matching source query found for table ${options.sourceTable.name}` }]
      };
    }

    return matchingQuery.evaluateRowWithErrors(options.sourceTable, options.record);
  }

  getSourceTables(): Set<TablePattern> {
    let result = new Set<TablePattern>();
    for (let query of this.sourceQueries) {
      result.add(query.sourceTable!);
    }
    return result;
  }

  tableTriggersEvent(table: SourceTableInterface): boolean {
    return this.sourceQueries.some((query) => query.applies(table));
  }
}
