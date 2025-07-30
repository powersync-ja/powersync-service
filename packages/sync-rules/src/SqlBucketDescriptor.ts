import { BucketInclusionReason, ResolvedBucket } from './BucketDescription.js';
import { BucketParameterQuerier, mergeBucketParameterQueriers } from './BucketParameterQuerier.js';
import { BucketSource, BucketSourceType, ResultSetDescription } from './BucketSource.js';
import { ColumnDefinition } from './ExpressionType.js';
import { IdSequence } from './IdSequence.js';
import { SourceTableInterface } from './SourceTableInterface.js';
import { SqlDataQuery } from './SqlDataQuery.js';
import { SqlParameterQuery } from './SqlParameterQuery.js';
import { GetQuerierOptions, SyncRulesOptions } from './SqlSyncRules.js';
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
  SourceSchema,
  SqliteRow
} from './types.js';

export interface QueryParseResult {
  /**
   * True if parsed in some form, even if there are errors.
   */
  parsed: boolean;

  errors: SqlRuleError[];
}

export class SqlBucketDescriptor implements BucketSource {
  name: string;
  bucketParameters?: string[];

  constructor(name: string) {
    this.name = name;
  }

  get type(): BucketSourceType {
    return BucketSourceType.SYNC_RULE;
  }

  public get subscribedToByDefault(): boolean {
    return true;
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
    const dataRows = SqlDataQuery.fromSql(this.name, this.bucketParameters, sql, options);

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

  /**
   * @deprecated Use `pushBucketParameterQueriers` instead and merge at the top-level.
   */
  getBucketParameterQuerier(options: GetQuerierOptions): BucketParameterQuerier {
    const queriers: BucketParameterQuerier[] = [];
    this.pushBucketParameterQueriers(queriers, options);

    return mergeBucketParameterQueriers(queriers);
  }

  pushBucketParameterQueriers(result: BucketParameterQuerier[], options: GetQuerierOptions) {
    const reasons = [this.bucketInclusionReason()];
    const staticBuckets = this.getStaticBucketDescriptions(options.globalParameters, reasons);
    const staticQuerier = {
      staticBuckets,
      hasDynamicBuckets: false,
      parameterQueryLookups: [],
      queryDynamicBucketDescriptions: async () => []
    } satisfies BucketParameterQuerier;
    result.push(staticQuerier);

    if (this.parameterQueries.length == 0) {
      return;
    }

    const dynamicQueriers = this.parameterQueries.map((query) =>
      query.getBucketParameterQuerier(options.globalParameters, reasons)
    );
    result.push(...dynamicQueriers);
  }

  getStaticBucketDescriptions(parameters: RequestParameters, reasons: BucketInclusionReason[]): ResolvedBucket[] {
    let results: ResolvedBucket[] = [];
    for (let query of this.globalParameterQueries) {
      for (const desc of query.getStaticBucketDescriptions(parameters)) {
        results.push({
          ...desc,
          inclusion_reasons: reasons
        });
      }
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

  private bucketInclusionReason(): BucketInclusionReason {
    return 'default';
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

  resolveResultSets(schema: SourceSchema, tables: Record<string, Record<string, ColumnDefinition>>) {
    for (let query of this.dataQueries) {
      const outTables = query.getColumnOutputs(schema);
      for (let table of outTables) {
        tables[table.name] ??= {};
        for (let column of table.columns) {
          if (column.name != 'id') {
            tables[table.name][column.name] ??= column;
          }
        }
      }
    }
  }

  debugWriteOutputTables(result: Record<string, { query: string }[]>): void {
    for (let q of this.dataQueries) {
      result[q.table!] ??= [];
      const r = {
        query: q.sql
      };

      result[q.table!].push(r);
    }
  }

  debugRepresentation() {
    let all_parameter_queries = [...this.parameterQueries.values()].flat();
    let all_data_queries = [...this.dataQueries.values()].flat();
    return {
      name: this.name,
      type: this.type.toString(),
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
