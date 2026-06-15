import { SourceTable } from '../storage/SourceTable.js';

export class RelationCache<T> {
  private cache = new Map<string | number, SourceTable[]>();
  private idFunction: (item: T | SourceTable) => string | number;

  constructor(idFunction: (item: T | SourceTable) => string | number) {
    this.idFunction = idFunction;
  }

  /**
   * Update a single table in-place - use when the snapshot status of the table has changed.
   */
  update(table: SourceTable) {
    const id = this.idFunction(table);
    const existing = this.cache.get(id) ?? [];
    const replacementIndex = existing.findIndex((candidate) => candidate.id == table.id);
    if (replacementIndex == -1) {
      this.cache.set(id, [...existing, table]);
    } else {
      const next = [...existing];
      next[replacementIndex] = table;
      this.cache.set(id, next);
    }
  }

  /**
   * Set the full set of tables for a specific reference.
   */
  updateAll(ref: T, tables: SourceTable[]) {
    const id = this.idFunction(ref);
    this.cache.set(id, tables);
  }

  getAll(source: T | SourceTable): SourceTable[] | undefined {
    const id = this.idFunction(source);
    return this.cache.get(id);
  }

  delete(source: T): boolean {
    const id = this.idFunction(source);
    return this.cache.delete(id);
  }
}
