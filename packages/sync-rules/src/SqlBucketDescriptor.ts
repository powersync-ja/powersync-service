import { BucketDescription, BucketInclusionReason, ResolvedBucket } from './BucketDescription.js';
import { BucketParameterQuerier, mergeBucketParameterQueriers } from './BucketParameterQuerier.js';
import { IdSequence } from './IdSequence.js';
import { SourceTableInterface } from './SourceTableInterface.js';
import { SqlDataQuery } from './SqlDataQuery.js';
import { SqlParameterQuery } from './SqlParameterQuery.js';
import { GetQuerierOptions, SyncRulesOptions } from './SqlSyncRules.js';
import { StaticSqlParameterQuery } from './StaticSqlParameterQuery.js';
import { StreamQuery } from './StreamQuery.js';
import { TablePattern } from './TablePattern.js';
import { TableValuedFunctionSqlParameterQuery } from './TableValuedFunctionSqlParameterQuery.js';
import { SqlRuleError } from './errors.js';
import {
  EvaluatedParametersResult,
  EvaluateRowOptions,
  EvaluationResult,
  QueryParseOptions,
  RequestParameters,
  SqliteRow,
  StreamParseOptions
} from './types.js';

export interface QueryParseResult {
  /**
   * True if parsed in some form, even if there are errors.
   */
  parsed: boolean;

  errors: SqlRuleError[];
}

export enum SqlBucketDescriptorType {
  SYNC_RULE,
  STREAM
}

export class SqlBucketDescriptor {
  name: string;
  bucketParameters?: string[];
  type: SqlBucketDescriptorType;
  subscribedToByDefault: boolean;

  constructor(name: string, type: SqlBucketDescriptorType) {
    this.name = name;
    this.type = type;

    // Sync-rule style buckets are subscribed to by default, streams are opt-in unless their definition says otherwise.
    this.subscribedToByDefault = type == SqlBucketDescriptorType.SYNC_RULE;
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

  addUnifiedStreamQuery(sql: string, options: StreamParseOptions): QueryParseResult {
    const [query, errors] = StreamQuery.fromSql(this.name, sql, options);
    for (const parameterQuery of query.inferredParameters) {
      if (parameterQuery instanceof StaticSqlParameterQuery) {
        this.globalParameterQueries.push(parameterQuery);
      } else {
        this.parameterQueries.push(parameterQuery);
      }
    }
    this.dataQueries.push(query.data);
    this.subscribedToByDefault = options.default ?? false;

    return {
      parsed: true,
      errors
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
  getBucketParameterQuerier(options: GetQuerierOptions, parameters: RequestParameters): BucketParameterQuerier {
    const queriers: BucketParameterQuerier[] = [];
    this.pushBucketParameterQueriers(queriers, options, parameters);

    return mergeBucketParameterQueriers(queriers);
  }

  pushBucketParameterQueriers(
    result: BucketParameterQuerier[],
    options: GetQuerierOptions,
    parameters: RequestParameters
  ) {
    const reasons = [this.bucketInclusionReason(options)];
    const staticBuckets = this.getStaticBucketDescriptions(parameters, reasons);
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

    const dynamicQueriers = this.parameterQueries.map((query) => query.getBucketParameterQuerier(parameters, reasons));
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

  private bucketInclusionReason(parameters: GetQuerierOptions): BucketInclusionReason {
    if (this.type == SqlBucketDescriptorType.STREAM && !this.subscribedToByDefault) {
      return { subscription: this.name };
    } else {
      return 'default';
    }
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
