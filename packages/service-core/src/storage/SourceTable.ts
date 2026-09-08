import {
  BucketDataSource,
  BucketDefinitionId,
  DEFAULT_TAG,
  EventDefinitionId,
  ParameterIndexId,
  ParameterIndexLookupCreator,
  SourceTableRef
} from '@powersync/service-sync-rules';
import { bson } from '../index.js';
import * as util from '../util/util-index.js';
import { ColumnDescriptor, JsonValue } from './SourceEntity.js';

/**
 * Format of the id depends on the bucket storage module. It should be consistent within the module.
 */
export type SourceTableId = string | bson.ObjectId;

/**
 * Compare source-table ids without coercing between storage-specific types.
 */
export function sourceTableIdEquals(left: SourceTableId, right: SourceTableId): boolean {
  if (typeof left === 'string' || typeof right === 'string') {
    return typeof left === 'string' && typeof right === 'string' && left === right;
  }
  return left.equals(right);
}

export interface SourceTableOptions {
  id: SourceTableId;
  ref: SourceTableRef;
  objectId: number | string | undefined;
  replicaIdColumns: ColumnDescriptor[];
  snapshotComplete: boolean;
  bucketDataSources: BucketDataSource[];
  parameterLookupSources: ParameterIndexLookupCreator[];
  bucketDataSourceIds?: Set<BucketDefinitionId>;
  parameterLookupSourceIds?: Set<ParameterIndexId>;
  /**
   * Compiled event definitions assigned to this persisted source-table record.
   *
   * Undefined is the legacy/non-incremental representation where event selection is
   * based on the table ref. V3 incremental storage always supplies this set.
   */
  eventDefinitionIds?: Set<EventDefinitionId>;
  /**
   * Source-specific metadata. Null when no metadata has been recorded.
   */
  sourceMetadata?: JsonValue;
}

export interface TableSnapshotStatus {
  totalEstimatedCount: number;
  replicatedCount: number;
  lastKey: Uint8Array | null;
}

/**
 * Represents a resolved source table.
 *
 * There could be multiple of these for the same SourceTableRef.
 * For that reason, we do not implement the SourceTableRef interface, to ensure that the two are not used interchangably.
 */
export class SourceTable {
  static readonly DEFAULT_TAG = DEFAULT_TAG;

  /**
   * True if the table is used in sync config for data queries.
   *
   * This value is resolved externally, and cached here.
   *
   * Defaults to true for tests.
   */
  public syncData = true;

  /**
   * True if the table is used in sync config for data queries.
   *
   * This value is resolved externally, and cached here.
   *
   * Defaults to true for tests.
   */
  public syncParameters = true;

  /**
   * True if this table should evaluate event definitions for row changes.
   *
   * This value is resolved externally, and cached here. V3 storage assigns disjoint
   * event-definition ids to SourceTables for the same physical table. Multiple records
   * may evaluate different events, but each event id is evaluated through at most one record.
   *
   * Defaults to true for tests.
   */
  public syncEvent = true;

  /**
   * True if raw data should be stored in current_data collection.
   *
   * This is needed when the source sends partial row data (e.g. TOAST values).
   * When REPLICA IDENTITY FULL is configured, complete rows are always sent,
   * so we don't need to store raw data.
   *
   * This value is resolved externally based on table configuration.
   *
   * Defaults to true for tests (conservative approach).
   */
  public storeCurrentData = true;

  /**
   * Always undefined if snapshotComplete = true.
   *
   * May be set if snapshotComplete = false.
   */
  public snapshotStatus: TableSnapshotStatus | undefined = undefined;

  public snapshotComplete: boolean;

  constructor(public readonly options: SourceTableOptions) {
    this.snapshotComplete = options.snapshotComplete;
  }

  get id() {
    return this.options.id;
  }

  get objectId() {
    return this.options.objectId;
  }

  get schema() {
    return this.options.ref.schema;
  }
  get name() {
    return this.options.ref.name;
  }

  get ref() {
    return this.options.ref;
  }

  get replicaIdColumns() {
    return this.options.replicaIdColumns;
  }

  get bucketDataSources() {
    return this.options.bucketDataSources;
  }

  get parameterLookupSources() {
    return this.options.parameterLookupSources;
  }

  get bucketDataSourceIds() {
    return this.options.bucketDataSourceIds;
  }

  get parameterLookupSourceIds() {
    return this.options.parameterLookupSourceIds;
  }

  get eventDefinitionIds() {
    return this.options.eventDefinitionIds;
  }

  get sourceMetadata() {
    return this.options.sourceMetadata ?? null;
  }

  /**
   * Sanitized name of the entity in the format of "{schema}.{entity name}".
   * Suitable for safe use in Postgres queries.
   */
  get qualifiedName() {
    return util.qualifiedName(this.ref);
  }

  get syncAny() {
    return this.syncData || this.syncParameters || this.syncEvent;
  }

  /**
   * In-memory clone of the table status.
   */
  clone() {
    return this.copyWithSourceMetadata(this.sourceMetadata);
  }

  /**
   * Copy this table with different source metadata, preserving its resolved state.
   */
  withSourceMetadata(sourceMetadata: JsonValue) {
    return this.copyWithSourceMetadata(sourceMetadata);
  }

  private copyWithSourceMetadata(sourceMetadata: JsonValue) {
    const copy = new SourceTable({
      id: this.id,
      ref: { ...this.options.ref },
      objectId: this.objectId,
      replicaIdColumns: this.replicaIdColumns.map((column) => ({ ...column })),
      snapshotComplete: this.snapshotComplete,
      bucketDataSources: [...this.bucketDataSources],
      parameterLookupSources: [...this.parameterLookupSources],
      bucketDataSourceIds: this.bucketDataSourceIds == null ? undefined : new Set(this.bucketDataSourceIds),
      parameterLookupSourceIds:
        this.parameterLookupSourceIds == null ? undefined : new Set(this.parameterLookupSourceIds),
      eventDefinitionIds: this.eventDefinitionIds == null ? undefined : new Set(this.eventDefinitionIds),
      sourceMetadata: structuredClone(sourceMetadata)
    });
    copy.syncData = this.syncData;
    copy.syncParameters = this.syncParameters;
    copy.syncEvent = this.syncEvent;
    copy.storeCurrentData = this.storeCurrentData;
    copy.snapshotStatus =
      this.snapshotStatus == null
        ? undefined
        : {
            ...this.snapshotStatus,
            lastKey: this.snapshotStatus.lastKey?.slice() ?? null
          };
    return copy;
  }

  formatSnapshotProgress() {
    if (this.snapshotComplete || this.snapshotStatus == null) {
      // Should not happen
      return '-';
    } else if (this.snapshotStatus.totalEstimatedCount < 0) {
      return `${this.snapshotStatus.replicatedCount}/?`;
    } else {
      return `${this.snapshotStatus.replicatedCount}/~${this.snapshotStatus.totalEstimatedCount}`;
    }
  }
}

/**
 * A cloned SourceTable exposed to reconciliation with public fields typed as read-only.
 * `options` is omitted so callers cannot mutate the underlying option bag without an explicit cast.
 */
export type SourceTableCandidate = Omit<Readonly<SourceTable>, 'options' | 'withSourceMetadata'> & {
  withSourceMetadata(sourceMetadata: JsonValue): SourceTableCandidate;
};
