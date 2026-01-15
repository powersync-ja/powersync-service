import { BaseSqlDataQuery } from '../BaseSqlDataQuery.js';
import { BucketPriority, DEFAULT_BUCKET_PRIORITY } from '../BucketDescription.js';
import {
  BucketDataSource,
  ParameterIndexLookupCreator,
  BucketSource,
  BucketSourceType,
  CreateSourceParams,
  HydratedBucketSource,
  BucketParameterQuerierSource
} from '../BucketSource.js';
import { ColumnDefinition } from '../ExpressionType.js';
import { SourceTableInterface } from '../SourceTableInterface.js';
import { TablePattern } from '../TablePattern.js';
import { EvaluateRowOptions, UnscopedEvaluationResult, SourceSchema, TableRow } from '../types.js';
import { StreamVariant } from './variant.js';

export class SyncStream implements BucketSource {
  name: string;
  subscribedToByDefault: boolean;
  priority: BucketPriority;
  variants: { variant: StreamVariant; dataSource: SyncStreamDataSource }[];
  data: BaseSqlDataQuery;

  public readonly dataSources: BucketDataSource[];
  public readonly parameterIndexLookupCreators: ParameterIndexLookupCreator[];

  constructor(name: string, data: BaseSqlDataQuery, variants: StreamVariant[]) {
    this.name = name;
    this.subscribedToByDefault = false;
    this.priority = DEFAULT_BUCKET_PRIORITY;
    this.variants = [];
    this.data = data;

    this.dataSources = [];
    this.parameterIndexLookupCreators = [];

    for (let variant of variants) {
      const dataSource = new SyncStreamDataSource(this, data, variant);
      this.dataSources.push(dataSource);
      this.variants.push({ variant, dataSource });
      const lookupCreators = variant.indexLookupCreators();
      this.parameterIndexLookupCreators.push(...lookupCreators);
    }
  }

  public get type(): BucketSourceType {
    return BucketSourceType.SYNC_STREAM;
  }

  debugRepresentation() {
    return {
      name: this.name,
      type: BucketSourceType[BucketSourceType.SYNC_STREAM],
      variants: this.variants.map(({ variant }) => variant.debugRepresentation()),
      data: {
        table: this.data.sourceTable,
        columns: this.data.columnOutputNames()
      }
    };
  }

  hydrate(params: CreateSourceParams): HydratedBucketSource {
    let queriers: BucketParameterQuerierSource[] = [];
    for (let { variant, dataSource } of this.variants) {
      const querier = variant.createParameterQuerierSource(params, this, dataSource);
      queriers.push(querier);
    }

    return {
      definition: this,
      pushBucketParameterQueriers(result, options) {
        for (let querier of queriers) {
          querier.pushBucketParameterQueriers(result, options);
        }
      }
    };
  }
}

export class SyncStreamDataSource implements BucketDataSource {
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

  public get uniqueName(): string {
    return this.variant.defaultBucketPrefix(this.stream.name);
  }

  getSourceTables() {
    return [this.data.sourceTable];
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

  evaluateRow(options: EvaluateRowOptions): UnscopedEvaluationResult[] {
    if (!this.data.applies(options.sourceTable)) {
      return [];
    }

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
      serializedBucketParameters: () => {
        return this.variant.bucketParametersForRow(row);
      }
    });
  }
}
