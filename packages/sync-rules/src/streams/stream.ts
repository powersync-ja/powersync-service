import { BaseSqlDataQuery } from '../BaseSqlDataQuery.js';
import { BucketPriority, DEFAULT_BUCKET_PRIORITY } from '../BucketDescription.js';
import {
  BucketDataSource,
  BucketDataSourceDefinition,
  BucketParameterLookupSourceDefinition,
  BucketParameterQuerierSourceDefinition,
  BucketSource,
  BucketSourceType,
  CreateSourceParams
} from '../BucketSource.js';
import { ColumnDefinition } from '../ExpressionType.js';
import { SourceTableInterface } from '../SourceTableInterface.js';
import { TablePattern } from '../TablePattern.js';
import { EvaluateRowOptions, EvaluationResult, SourceSchema, TableRow } from '../types.js';
import { StreamVariant } from './variant.js';

export class SyncStream implements BucketSource {
  name: string;
  subscribedToByDefault: boolean;
  priority: BucketPriority;
  variants: StreamVariant[];
  data: BaseSqlDataQuery;

  public readonly dataSources: BucketDataSourceDefinition[];
  public readonly parameterLookupSources: BucketParameterLookupSourceDefinition[];
  public readonly parameterQuerierSources: BucketParameterQuerierSourceDefinition[];

  constructor(name: string, data: BaseSqlDataQuery, variants: StreamVariant[]) {
    this.name = name;
    this.subscribedToByDefault = false;
    this.priority = DEFAULT_BUCKET_PRIORITY;
    this.variants = variants;
    this.data = data;

    this.dataSources = [];
    this.parameterLookupSources = [];
    this.parameterQuerierSources = [];

    for (let variant of variants) {
      const dataSource = new SyncStreamDataSource(this, data, variant);
      this.dataSources.push(dataSource);
      const lookupSources = variant.lookupSources();
      this.parameterQuerierSources.push(variant.querierSource(this, dataSource));
      this.parameterLookupSources.push(...lookupSources);
    }
  }

  public get type(): BucketSourceType {
    return BucketSourceType.SYNC_STREAM;
  }

  debugRepresentation() {
    return {
      name: this.name,
      type: BucketSourceType[BucketSourceType.SYNC_STREAM],
      variants: this.variants.map((v) => v.debugRepresentation()),
      data: {
        table: this.data.sourceTable,
        columns: this.data.columnOutputNames()
      }
    };
  }
}

export class SyncStreamDataSource implements BucketDataSourceDefinition {
  constructor(
    private stream: SyncStream,
    private data: BaseSqlDataQuery,
    private variant: StreamVariant
  ) {}

  /**
   * Not relevant for sync streams.
   */
  get bucketParameters() {
    return [];
  }

  public get defaultBucketPrefix(): string {
    return this.variant.defaultBucketPrefix(this.stream.name);
  }

  getSourceTables(): Set<TablePattern> {
    return new Set<TablePattern>([this.data.sourceTable]);
  }

  tableSyncsData(table: SourceTableInterface): boolean {
    return this.data.applies(table);
  }

  resolveResultSets(schema: SourceSchema, tables: Record<string, Record<string, ColumnDefinition>>): void {
    return this.data.resolveResultSets(schema, tables);
  }

  debugWriteOutputTables(result: Record<string, { query: string }[]>): void {
    result[this.data.table!.sqlName] ??= [];
    const r = {
      query: this.data.sql
    };

    result[this.data.table!.sqlName].push(r);
  }

  createDataSource(params: CreateSourceParams): BucketDataSource {
    const hydrationState = params.hydrationState;
    const bucketPrefix = hydrationState.getBucketSourceState(this).bucketPrefix;
    return {
      evaluateRow: (options: EvaluateRowOptions): EvaluationResult[] => {
        if (!this.data.applies(options.sourceTable)) {
          return [];
        }

        const stream = this.stream;
        const row: TableRow = {
          sourceTable: options.sourceTable,
          record: options.record
        };

        // There is some duplication in work here when there are multiple variants on a stream:
        // Each variant does the same row transformation (only the filters / bucket ids differ).
        // However, architecturally we do need to be able to evaluate each variant separately.
        return this.data.evaluateRowWithOptions({
          table: options.sourceTable,
          row: options.record,
          bucketIds: () => {
            return this.variant.bucketIdsForRow(bucketPrefix, row);
          }
        });
      }
    };
  }
}
