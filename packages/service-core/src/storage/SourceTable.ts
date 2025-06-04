import { DEFAULT_TAG } from '@powersync/service-sync-rules';
import * as util from '../util/util-index.js';
import { ColumnDescriptor } from './SourceEntity.js';

export interface TableSnapshotStatus {
  totalEstimatedCount: number;
  replicatedCount: number;
  lastKey: Uint8Array | null;
}

export class SourceTable {
  static readonly DEFAULT_TAG = DEFAULT_TAG;

  /**
   * True if the table is used in sync rules for data queries.
   *
   * This value is resolved externally, and cached here.
   *
   * Defaults to true for tests.
   */
  public syncData = true;

  /**
   * True if the table is used in sync rules for data queries.
   *
   * This value is resolved externally, and cached here.
   *
   * Defaults to true for tests.
   */
  public syncParameters = true;

  /**
   * True if the table is used in sync rules for events.
   *
   * This value is resolved externally, and cached here.
   *
   * Defaults to true for tests.
   */
  public syncEvent = true;

  /**
   * Always undefined if snapshotComplete = true.
   *
   * May be set if snapshotComplete = false.
   */
  public snapshotStatus: TableSnapshotStatus | undefined = undefined;

  constructor(
    public readonly id: any,
    public readonly connectionTag: string,
    public readonly objectId: number | string | undefined,
    public readonly schema: string,
    public readonly table: string,

    public readonly replicaIdColumns: ColumnDescriptor[],
    public snapshotComplete: boolean
  ) {}

  get hasReplicaIdentity() {
    return this.replicaIdColumns.length > 0;
  }

  /**
   * Use for postgres only.
   *
   * Usage: db.query({statement: `SELECT $1::regclass`, params: [{type: 'varchar', value: table.qualifiedName}]})
   */
  get qualifiedName() {
    return this.escapedIdentifier;
  }

  /**
   * Use for postgres and logs only.
   *
   * Usage: db.query(`SELECT * FROM ${table.escapedIdentifier}`)
   */
  get escapedIdentifier() {
    return `${util.escapeIdentifier(this.schema)}.${util.escapeIdentifier(this.table)}`;
  }

  get syncAny() {
    return this.syncData || this.syncParameters || this.syncEvent;
  }

  /**
   * In-memory clone of the table status.
   */
  clone() {
    const copy = new SourceTable(
      this.id,
      this.connectionTag,
      this.objectId,
      this.schema,
      this.table,
      this.replicaIdColumns,
      this.snapshotComplete
    );
    copy.syncData = this.syncData;
    copy.syncParameters = this.syncParameters;
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
