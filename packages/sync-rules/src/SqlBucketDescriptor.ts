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
  bucket_parameters?: string[];

  constructor(
    name: string,
    public idSequence: IdSequence
  ) {
    this.name = name;
  }

  /**
   * source table -> queries
   */
  data_queries: SqlDataQuery[] = [];
  parameter_queries: SqlParameterQuery[] = [];
  global_parameter_queries: (StaticSqlParameterQuery | TableValuedFunctionSqlParameterQuery)[] = [];

  parameterIdSequence = new IdSequence();

  addDataQuery(sql: string, options: SyncRulesOptions): QueryParseResult {
    if (this.bucket_parameters == null) {
      throw new Error('Bucket parameters must be defined');
    }
    const dataRows = SqlDataQuery.fromSql(this.name, this.bucket_parameters, sql, options, this.idSequence.nextId());

    this.data_queries.push(dataRows);

    return {
      parsed: true,
      errors: dataRows.errors
    };
  }

  addParameterQuery(sql: string, options: QueryParseOptions): QueryParseResult {
    const parameterQuery = SqlParameterQuery.fromSql(this.name, sql, options, this.parameterIdSequence.nextId());
    if (this.bucket_parameters == null) {
      this.bucket_parameters = parameterQuery.bucket_parameters;
    } else {
      if (
        new Set([...parameterQuery.bucket_parameters!, ...this.bucket_parameters]).size != this.bucket_parameters.length
      ) {
        throw new Error('Bucket parameters must match for each parameter query within a bucket');
      }
    }
    if (parameterQuery instanceof SqlParameterQuery) {
      this.parameter_queries.push(parameterQuery);
    } else {
      this.global_parameter_queries.push(parameterQuery);
    }

    return {
      parsed: true,
      errors: parameterQuery.errors
    };
  }

  evaluateRow(options: EvaluateRowOptions): EvaluationResult[] {
    let results: EvaluationResult[] = [];
    for (let query of this.data_queries) {
      if (!query.applies(options.sourceTable)) {
        continue;
      }

      results.push(...query.evaluateRow(options.sourceTable, options.record));
    }
    return results;
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

  getBucketParameterQuerier(parameters: RequestParameters): BucketParameterQuerier {
    const staticBuckets = this.getStaticBucketDescriptions(parameters);
    const staticQuerier = {
      staticBuckets,
      hasDynamicBuckets: false,
      parameterQueryLookups: [],
      queryDynamicBucketDescriptions: async () => []
    } satisfies BucketParameterQuerier;

    if (this.parameter_queries.length == 0) {
      return staticQuerier;
    }

    const dynamicQueriers = this.parameter_queries.map((query) => query.getBucketParameterQuerier(parameters));
    return mergeBucketParameterQueriers([staticQuerier, ...dynamicQueriers]);
  }

  getStaticBucketDescriptions(parameters: RequestParameters): BucketDescription[] {
    let results: BucketDescription[] = [];
    for (let query of this.global_parameter_queries) {
      results.push(...query.getStaticBucketDescriptions(parameters));
    }
    return results;
  }

  hasDynamicBucketQueries(): boolean {
    return this.parameter_queries.length > 0;
  }

  getSourceTables(): Set<TablePattern> {
    let result = new Set<TablePattern>();
    for (let query of this.parameter_queries) {
      result.add(query.sourceTable!);
    }
    for (let query of this.data_queries) {
      result.add(query.sourceTable!);
    }

    // Note: No physical tables for global_parameter_queries

    return result;
  }

  tableSyncsData(table: SourceTableInterface): boolean {
    for (let query of this.data_queries) {
      if (query.applies(table)) {
        return true;
      }
    }
    return false;
  }

  tableSyncsParameters(table: SourceTableInterface): boolean {
    for (let query of this.parameter_queries) {
      if (query.applies(table)) {
        return true;
      }
    }
    return false;
  }
}
