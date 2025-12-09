import { BucketInclusionReason, ResolvedBucket } from './BucketDescription.js';
import { BucketParameterQuerier, mergeBucketParameterQueriers, PendingQueriers } from './BucketParameterQuerier.js';
import {
  BucketDataSource,
  BucketDataSourceDefinition,
  BucketParameterSource,
  BucketParameterSourceDefinition,
  BucketSourceType,
  CreateSourceParams,
  ResultSetDescription
} from './BucketSource.js';
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
import { CompatibilityContext } from './compatibility.js';
import {
  BucketIdTransformer,
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

export class SqlBucketDescriptor implements BucketDataSourceDefinition, BucketParameterSourceDefinition {
  name: string;
  private bucketParametersInternal: string[] | null = null;

  constructor(name: string) {
    this.name = name;
  }

  get type(): BucketSourceType {
    return BucketSourceType.SYNC_RULE;
  }

  public get subscribedToByDefault(): boolean {
    return true;
  }

  public get bucketParameters(): string[] {
    return this.bucketParametersInternal ?? [];
  }

  /**
   * source table -> queries
   */
  dataQueries: SqlDataQuery[] = [];
  parameterQueries: SqlParameterQuery[] = [];
  globalParameterQueries: (StaticSqlParameterQuery | TableValuedFunctionSqlParameterQuery)[] = [];

  parameterIdSequence = new IdSequence();

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
      definition: this,
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

  createParameterSource(params: CreateSourceParams): BucketParameterSource {
    return {
      definition: this,

      evaluateParameterRow: (sourceTable: SourceTableInterface, row: SqliteRow): EvaluatedParametersResult[] => {
        let results: EvaluatedParametersResult[] = [];
        for (let query of this.parameterQueries) {
          if (query.applies(sourceTable)) {
            results.push(...query.evaluateParameterRow(row));
          }
        }
        return results;
      },
      pushBucketParameterQueriers: (result: PendingQueriers, options: GetQuerierOptions) => {
        const reasons = [this.bucketInclusionReason()];
        const staticBuckets = this.getStaticBucketDescriptions(
          options.globalParameters,
          reasons,
          params.bucketIdTransformer
        );
        const staticQuerier = {
          staticBuckets,
          hasDynamicBuckets: false,
          parameterQueryLookups: [],
          queryDynamicBucketDescriptions: async () => []
        } satisfies BucketParameterQuerier;
        result.queriers.push(staticQuerier);

        if (this.parameterQueries.length == 0) {
          return;
        }

        const dynamicQueriers = this.parameterQueries.map((query) =>
          query.getBucketParameterQuerier(options.globalParameters, reasons, params.bucketIdTransformer)
        );
        result.queriers.push(...dynamicQueriers);
      },

      /**
       * @deprecated Use `pushBucketParameterQueriers` instead and merge at the top-level.
       */
      getBucketParameterQuerier(options: GetQuerierOptions): BucketParameterQuerier {
        const queriers: BucketParameterQuerier[] = [];
        this.pushBucketParameterQueriers({ queriers, errors: [] }, options);

        return mergeBucketParameterQueriers(queriers);
      }
    };
  }

  getStaticBucketDescriptions(
    parameters: RequestParameters,
    reasons: BucketInclusionReason[],
    transformer: BucketIdTransformer
  ): ResolvedBucket[] {
    let results: ResolvedBucket[] = [];
    for (let query of this.globalParameterQueries) {
      for (const desc of query.getStaticBucketDescriptions(parameters, transformer)) {
        results.push({
          ...desc,
          definition: this.name,
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
