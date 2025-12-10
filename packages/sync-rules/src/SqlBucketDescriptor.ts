import {
  BucketDataSource,
  BucketDataSourceDefinition,
  BucketSource,
  BucketSourceType,
  CreateSourceParams,
  DebugMergedSource,
  mergeParameterLookupSources,
  mergeParameterQuerierSources
} from './BucketSource.js';
import { ColumnDefinition } from './ExpressionType.js';
import { IdSequence } from './IdSequence.js';
import { SourceTableInterface } from './SourceTableInterface.js';
import { SqlDataQuery } from './SqlDataQuery.js';
import { SqlParameterQuery } from './SqlParameterQuery.js';
import { SyncRulesOptions } from './SqlSyncRules.js';
import { StaticSqlParameterQuery } from './StaticSqlParameterQuery.js';
import { TablePattern } from './TablePattern.js';
import { TableValuedFunctionSqlParameterQuery } from './TableValuedFunctionSqlParameterQuery.js';
import { CompatibilityContext } from './compatibility.js';
import { SqlRuleError } from './errors.js';
import { EvaluationResult, QueryParseOptions, SourceSchema } from './types.js';

export interface QueryParseResult {
  /**
   * True if parsed in some form, even if there are errors.
   */
  parsed: boolean;

  errors: SqlRuleError[];
}

export class SqlBucketDescriptor implements BucketDataSourceDefinition, BucketSource {
  name: string;
  private bucketParametersInternal: string[] | null = null;

  public readonly subscribedToByDefault: boolean = true;

  /**
   * source table -> queries
   */
  dataQueries: SqlDataQuery[] = [];
  parameterQueries: SqlParameterQuery[] = [];
  globalParameterQueries: (StaticSqlParameterQuery | TableValuedFunctionSqlParameterQuery)[] = [];

  parameterIdSequence = new IdSequence();

  constructor(name: string) {
    this.name = name;
  }

  get type(): BucketSourceType {
    return BucketSourceType.SYNC_RULE;
  }

  public get bucketParameters(): string[] {
    return this.bucketParametersInternal ?? [];
  }

  get dataSources() {
    return [this];
  }

  get parameterLookupSources() {
    return this.parameterQueries;
  }

  get parameterQuerierSources() {
    return [...this.parameterQueries, ...this.globalParameterQueries];
  }

  addDataQuery(sql: string, options: SyncRulesOptions, compatibility: CompatibilityContext): QueryParseResult {
    if (this.bucketParametersInternal == null) {
      throw new Error('Bucket parameters must be defined');
    }
    const dataRows = SqlDataQuery.fromSql(this.name, this.bucketParametersInternal, sql, options, compatibility);

    this.dataQueries.push(dataRows);

    return {
      parsed: true,
      errors: dataRows.errors
    };
  }

  addParameterQuery(sql: string, options: QueryParseOptions): QueryParseResult {
    const parameterQuery = SqlParameterQuery.fromSql(this.name, sql, options, this.parameterIdSequence.nextId());
    if (this.bucketParametersInternal == null) {
      this.bucketParametersInternal = parameterQuery.bucketParameters;
    } else {
      if (
        new Set([...parameterQuery.bucketParameters!, ...this.bucketParametersInternal]).size !=
        this.bucketParametersInternal.length
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

  createDataSource(params: CreateSourceParams): BucketDataSource {
    return {
      evaluateRow: (options) => {
        let results: EvaluationResult[] = [];
        for (let query of this.dataQueries) {
          if (!query.applies(options.sourceTable)) {
            continue;
          }

          results.push(...query.evaluateRow(options.sourceTable, options.record, params.bucketIdTransformer));
        }
        return results;
      }
    };
  }

  getSourceTables(): Set<TablePattern> {
    let result = new Set<TablePattern>();
    for (let query of this.parameterQueries) {
      result.add(query.sourceTable);
    }
    for (let query of this.dataQueries) {
      result.add(query.sourceTable);
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

  resolveResultSets(schema: SourceSchema, tables: Record<string, Record<string, ColumnDefinition>>) {
    for (let query of this.dataQueries) {
      query.resolveResultSets(schema, tables);
    }
  }

  debugWriteOutputTables(result: Record<string, { query: string }[]>): void {
    for (let q of this.dataQueries) {
      result[q.table!.sqlName] ??= [];
      const r = {
        query: q.sql
      };

      result[q.table!.sqlName].push(r);
    }
  }

  debugRepresentation() {
    let all_parameter_queries = [...this.parameterQueries.values()].flat();
    let all_data_queries = [...this.dataQueries.values()].flat();
    return {
      name: this.name,
      type: BucketSourceType[this.type],
      bucket_parameters: this.bucketParameters,
      global_parameter_queries: this.globalParameterQueries.map((q) => {
        return {
          sql: q.sql
        };
      }),
      parameter_queries: all_parameter_queries.map((q) => {
        return {
          sql: q.sql,
          table: q.sourceTable,
          input_parameters: q.inputParameters
        };
      }),
      data_queries: all_data_queries.map((q) => {
        return {
          sql: q.sql,
          table: q.sourceTable,
          columns: q.columnOutputNames()
        };
      })
    };
  }
}
