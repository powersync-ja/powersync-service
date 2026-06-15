import {
  BucketDataSource,
  DEFAULT_TAG,
  ParameterIndexLookupCreator,
  SourceTableRef
} from '@powersync/service-sync-rules';
import { bson } from '../index.js';
import * as util from '../util/util-index.js';
import { ColumnDescriptor } from './SourceEntity.js';

/**
 * Format of the id depends on the bucket storage module. It should be consistent within the module.
 */
export type SourceTableId = string | bson.ObjectId;

export interface SourceTableOptions {
  id: SourceTableId;
  ref: SourceTableRef;
  objectId: number | string | undefined;
  replicaIdColumns: ColumnDescriptor[];
  snapshotComplete: boolean;
  bucketDataSources: BucketDataSource[];
  parameterLookupSources: ParameterIndexLookupCreator[];
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
   * True if the table is used in sync config for events.
   *
   * This value is resolved externally, and cached here.
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
    const copy = new SourceTable({
      id: this.id,
      ref: this.options.ref,
      objectId: this.objectId,
      replicaIdColumns: this.replicaIdColumns,
      snapshotComplete: this.snapshotComplete,
      bucketDataSources: this.bucketDataSources,
      parameterLookupSources: this.parameterLookupSources
    });
    copy.syncData = this.syncData;
    copy.syncParameters = this.syncParameters;
    copy.syncEvent = this.syncEvent;
    copy.storeCurrentData = this.storeCurrentData;
    copy.snapshotStatus = this.snapshotStatus;
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
