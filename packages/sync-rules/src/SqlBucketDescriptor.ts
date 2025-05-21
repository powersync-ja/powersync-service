import { BucketDescription } from './BucketDescription.js';
import { BucketParameterQuerier, mergeBucketParameterQueriers } from './BucketParameterQuerier.js';
import { IdSequence } from './IdSequence.js';
import { SourceTableInterface } from './SourceTableInterface.js';
import { SqlDataQuery } from './SqlDataQuery.js';
import { SqlParameterQuery } from './SqlParameterQuery.js';
import { SyncRulesOptions } from './SqlSyncRules.js';
import { StaticSqlParameterQuery } from './StaticSqlParameterQuery.js';
import { TablePattern } from './TablePattern.js';
import { TableValuedFunctionSqlParameterQuery } from './TableValuedFunctionSqlParameterQuery.js';
import { SqlRuleError } from './errors.js';
import {
  EvaluatedParametersResult,
  EvaluateRowOptions,
  EvaluationResult,
  QueryParseOptions,
  RequestParameters,
  SqliteRow
} from './types.js';

export interface QueryParseResult {
  /**
   * True if parsed in some form, even if there are errors.
   */
  parsed: boolean;

  errors: SqlRuleError[];
}

export class SqlBucketDescriptor {
  name: string;
  bucketParameters?: string[];

  constructor(
    name: string,
    public idSequence: IdSequence
  ) {
    this.name = name;
  }

  /**
   * source table -> queries
   */
  dataQueries: SqlDataQuery[] = [];
  parameterQueries: SqlParameterQuery[] = [];
  globalParameterQueries: (StaticSqlParameterQuery | TableValuedFunctionSqlParameterQuery)[] = [];

  parameterIdSequence = new IdSequence();

  addDataQuery(sql: string, options: SyncRulesOptions): QueryParseResult {
    if (this.bucketParameters == null) {
      throw new Error('Bucket parameters must be defined');
    }
    const dataRows = SqlDataQuery.fromSql(this.name, this.bucketParameters, sql, options, this.idSequence.nextId());

    this.dataQueries.push(dataRows);

    return {
      parsed: true,
      errors: dataRows.errors
    };
  }

  addParameterQuery(sql: string, options: QueryParseOptions): QueryParseResult {
    const parameterQuery = SqlParameterQuery.fromSql(this.name, sql, options, this.parameterIdSequence.nextId());
    if (this.bucketParameters == null) {
      this.bucketParameters = parameterQuery.bucketParameters;
    } else {
      if (
        new Set([...parameterQuery.bucketParameters!, ...this.bucketParameters]).size != this.bucketParameters.length
      ) {
        throw new Error('Bucket parameters must match for each parameter query within a bucket');
      }
    }
    if (parameterQuery instanceof SqlParameterQuery) {
      this.parameterQueries.push(parameterQuery);
    } else {
      this.globalParameterQueries.push(parameterQuery);
    }

    return {
      parsed: true,
      errors: parameterQuery.errors
    };
  }

  evaluateRow(options: EvaluateRowOptions): EvaluationResult[] {
    let results: EvaluationResult[] = [];
    for (let query of this.dataQueries) {
      if (!query.applies(options.sourceTable)) {
        continue;
      }

      results.push(...query.evaluateRow(options.sourceTable, options.record));
    }
    return results;
  }

  evaluateParameterRow(sourceTable: SourceTableInterface, row: SqliteRow): EvaluatedParametersResult[] {
    let results: EvaluatedParametersResult[] = [];
    for (let query of this.parameterQueries) {
      if (query.applies(sourceTable)) {
        results.push(...query.evaluateParameterRow(row));
      }
    }
    return results;
  }

  getBucketParameterQuerier(parameters: RequestParameters): BucketParameterQuerier {
    const staticBuckets = this.getStaticBucketDescriptions(parameters);
    const staticQuerier = {
      staticBuckets,
      hasDynamicBuckets: false,
      parameterQueryLookups: [],
      queryDynamicBucketDescriptions: async () => []
    } satisfies BucketParameterQuerier;

    if (this.parameterQueries.length == 0) {
      return staticQuerier;
    }

    const dynamicQueriers = this.parameterQueries.map((query) => query.getBucketParameterQuerier(parameters));
    return mergeBucketParameterQueriers([staticQuerier, ...dynamicQueriers]);
  }

  getStaticBucketDescriptions(parameters: RequestParameters): BucketDescription[] {
    let results: BucketDescription[] = [];
    for (let query of this.globalParameterQueries) {
      results.push(...query.getStaticBucketDescriptions(parameters));
    }
    return results;
  }

  hasDynamicBucketQueries(): boolean {
    return this.parameterQueries.length > 0;
  }

  getSourceTables(): Set<TablePattern> {
    let result = new Set<TablePattern>();
    for (let query of this.parameterQueries) {
      result.add(query.sourceTable!);
    }
    for (let query of this.dataQueries) {
      result.add(query.sourceTable!);
    }

    // Note: No physical tables for global_parameter_queries

    return result;
  }

  tableSyncsData(table: SourceTableInterface): boolean {
    for (let query of this.dataQueries) {
      if (query.applies(table)) {
        return true;
      }
    }
    return false;
  }

  tableSyncsParameters(table: SourceTableInterface): boolean {
    for (let query of this.parameterQueries) {
      if (query.applies(table)) {
        return true;
      }
    }
    return false;
  }
}
