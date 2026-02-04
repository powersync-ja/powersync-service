import { SourceTable } from '@powersync/service-core';
import { escapeIdentifier, toQualifiedTableName } from '../utils/mssql.js';
import { ServiceAssertionError } from '@powersync/service-errors';
import { CaptureInstance } from './CaptureInstance.js';

/**
 *  The cdc schema in SQL Server is reserved and created when enabling CDC on a database.
 */
export const CDC_SCHEMA = 'cdc';

export class MSSQLSourceTable {
  /**
   *  The unique name of the CDC capture instance for this table
   */
  public captureInstance: CaptureInstance | null = null;

  constructor(public sourceTable: SourceTable) {}

  updateSourceTable(updated: SourceTable): void {
    this.sourceTable = updated;
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
      throw new ServiceAssertionError(`No capture instance set for table: ${this.sourceTable.name}`);
    }
    return `${CDC_SCHEMA}.fn_cdc_get_all_changes_${this.captureInstance.name}`;
  }

  get netChangesFunction() {
    if (!this.captureInstance) {
      throw new ServiceAssertionError(`No capture instance set for table: ${this.sourceTable.name}`);
    }
    return `${CDC_SCHEMA}.fn_cdc_get_net_changes_${this.captureInstance.name}`;
  }

  /**
   *  Return the object ID of the source table.
   *  Object IDs in SQL Server are always numbers.
   */
  get objectId(): number {
    return this.sourceTable.objectId as number;
  }

  /**
   *  Escapes this source table's name and schema for use in MSSQL queries.
   */
  toQualifiedName(): string {
    return toQualifiedTableName(this.sourceTable.schema, this.sourceTable.name);
  }
}
