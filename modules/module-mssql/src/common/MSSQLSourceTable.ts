import { SourceEntityDescriptor, SourceTable } from '@powersync/service-core';
import { ServiceAssertionError } from '@powersync/service-errors';
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

  constructor(ref: SourceEntityDescriptor, sourceTables: SourceTable[]) {
    this.sourceTables = sourceTables;
    this.ref = ref;
  }

  updateSourceTable(updated: SourceTable): void {
    const index = this.sourceTables.findIndex((table) => table.id == updated.id);
    if (index == -1) {
      throw new ServiceAssertionError(`No SourceTable found for table: ${updated.id}`);
    }
    this.sourceTables[index] = updated;
  }

  getReplicatedSourceTables(): SourceTable[] {
    return this.sourceTables.filter((sourceTable) => sourceTable.syncAny);
  }

  enabledForCDC(): boolean {
    return this.captureInstance !== null;
  }

  setCaptureInstance(captureInstance: CaptureInstance) {
    this.captureInstance = captureInstance;
  }

  clearCaptureInstance() {
    this.captureInstance = null;
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
