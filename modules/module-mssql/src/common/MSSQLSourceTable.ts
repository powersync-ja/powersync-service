import { SourceEntityDescriptor, SourceTable } from '@powersync/service-core';
import { ServiceAssertionError } from '@powersync/service-errors';
import { readCaptureMetadata } from '../replication/CaptureReconciler.js';
import { toQualifiedTableName } from '../utils/mssql.js';
import { CaptureInstance } from './CaptureInstance.js';

/**
 *  The cdc schema in SQL Server is reserved and created when enabling CDC on a database.
 */
export const CDC_SCHEMA = 'cdc';

/**
 * Represents one underlying CDC capture instance.
 *
 * There could be multiple SourceTables associated with the same underlying capture instance.
 */
export class MSSQLSourceTable {
  /**
   *  The unique name of the CDC capture instance for this table
   */
  public captureInstance: CaptureInstance | null = null;

  /**
   * Can be 0, 1 or multiple SourceTables.
   */
  public readonly sourceTables: SourceTable[];

  public readonly ref: SourceEntityDescriptor;

  /**
   * Decoded once, since the persisted metadata only changes when a SourceTable is replaced.
   */
  private captureObjectId: number | null;

  constructor(ref: SourceEntityDescriptor, sourceTables: SourceTable[]) {
    this.sourceTables = sourceTables;
    this.ref = ref;
    this.captureObjectId = readPinnedCaptureObjectId(sourceTables);
  }

  updateSourceTable(updated: SourceTable): void {
    const index = this.sourceTables.findIndex((table) => table.id == updated.id);
    if (index == -1) {
      throw new ServiceAssertionError(`No SourceTable found for table: ${updated.id}`);
    }
    this.sourceTables[index] = updated;
    this.captureObjectId = readPinnedCaptureObjectId(this.sourceTables);
  }

  getReplicatedSourceTables(): SourceTable[] {
    return this.sourceTables.filter((sourceTable) => sourceTable.syncAny);
  }

  enabledForCDC(): boolean {
    return this.captureInstance !== null;
  }

  /**
   * Persisted capture-table object id, or null for a legacy binding.
   */
  get pinnedCaptureObjectId(): number | null {
    return this.captureObjectId;
  }

  /**
   * Bind the available capture instance matching this table's persisted identity.
   */
  setCaptureInstance(availableInstances: readonly CaptureInstance[]): void {
    this.captureInstance =
      availableInstances.find((instance) => instance.objectId === this.pinnedCaptureObjectId) ?? null;
  }

  get allChangesFunction() {
    if (!this.captureInstance) {
      throw new ServiceAssertionError(`No capture instance set for table: ${this.ref.name}`);
    }
    return `${CDC_SCHEMA}.fn_cdc_get_all_changes_${this.captureInstance.name}`;
  }

  get netChangesFunction() {
    if (!this.captureInstance) {
      throw new ServiceAssertionError(`No capture instance set for table: ${this.ref.name}`);
    }
    return `${CDC_SCHEMA}.fn_cdc_get_net_changes_${this.captureInstance.name}`;
  }

  /**
   *  Return the object ID of the source table.
   *  Object IDs in SQL Server are always numbers.
   */
  get objectId(): number {
    return this.ref.objectId as number;
  }

  /**
   *  Escapes this source table's name and schema for use in MSSQL queries.
   */
  toQualifiedName(): string {
    return toQualifiedTableName(this.ref.schema, this.ref.name);
  }
}

/**
 * The pinned capture-table object id shared by these records, or null for a legacy binding.
 */
function readPinnedCaptureObjectId(sourceTables: SourceTable[]): number | null {
  for (const sourceTable of sourceTables) {
    const metadata = readCaptureMetadata(sourceTable.sourceMetadata);
    if (metadata != null) {
      return metadata.captureTableObjectId;
    }
  }
  return null;
}
