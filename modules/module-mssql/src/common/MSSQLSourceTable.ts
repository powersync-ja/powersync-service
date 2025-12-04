import { SourceTable } from '@powersync/service-core';
import { escapeIdentifier, toQualifiedTableName } from '../utils/mssql.js';

export interface CaptureInstance {
  name: string;
  schema: string;
}

export interface MSSQLSourceTableOptions {
  sourceTable: SourceTable;
  /**
   *  The unique name of the CDC capture instance for this table
   */
  captureInstance: CaptureInstance;
}

export class MSSQLSourceTable {
  constructor(private options: MSSQLSourceTableOptions) {}

  get sourceTable() {
    return this.options.sourceTable;
  }

  updateSourceTable(updated: SourceTable): void {
    this.options.sourceTable = updated;
  }

  get captureInstance() {
    return this.options.captureInstance.name;
  }

  get cdcSchema() {
    return this.options.captureInstance.schema;
  }

  get CTTable() {
    return `${escapeIdentifier(this.cdcSchema)}.${this.captureInstance}_CT`;
  }

  get allChangesFunction() {
    return `${escapeIdentifier(this.cdcSchema)}.fn_cdc_get_all_changes_${this.captureInstance}`;
  }

  get netChangesFunction() {
    return `${escapeIdentifier(this.cdcSchema)}.fn_cdc_get_net_changes_${this.captureInstance}`;
  }

  /**
   *  Escapes this source table's name and schema for use in MSSQL queries.
   */
  toQualifiedName(): string {
    return toQualifiedTableName(this.sourceTable.schema, this.sourceTable.name);
  }
}
