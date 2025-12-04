import { SourceTable } from '@powersync/service-core';
import { MSSQLSourceTable } from './MSSQLSourceTable.js';
import { ServiceAssertionError } from '@powersync/service-errors';

export class MSSQLSourceTableCache {
  private cache = new Map<number | string, MSSQLSourceTable>();

  set(table: MSSQLSourceTable): void {
    this.cache.set(table.sourceTable.objectId!, table);
  }

  /**
   *  Updates the underlying source table of the cached MSSQLSourceTable.
   *  @param updatedTable
   */
  updateSourceTable(updatedTable: SourceTable) {
    const existingTable = this.cache.get(updatedTable.objectId!);

    if (!existingTable) {
      throw new ServiceAssertionError('Tried to update a non-existing MSSQLSourceTable in the cache');
    }
    existingTable.updateSourceTable(updatedTable);
  }

  get(tableId: number): MSSQLSourceTable | undefined {
    return this.cache.get(tableId);
  }

  getAll(): MSSQLSourceTable[] {
    return Array.from(this.cache.values());
  }

  delete(tableId: number): boolean {
    return this.cache.delete(tableId);
  }
}
