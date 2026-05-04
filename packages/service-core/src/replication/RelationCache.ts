import { SourceTable } from '../storage/SourceTable.js';

export class RelationCache<T> {
  private cache = new Map<string | number, SourceTable[]>();
  private idFunction: (item: T | SourceTable) => string | number;

  constructor(idFunction: (item: T | SourceTable) => string | number) {
    this.idFunction = idFunction;
  }

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

  updateAll(tables: SourceTable[]) {
    if (tables.length == 0) {
      return;
    }
    const id = this.idFunction(tables[0]);
    this.cache.set(id, tables);
  }

  get(source: T | SourceTable): SourceTable | undefined {
    return this.getAll(source)[0];
  }

  getAll(source: T | SourceTable): SourceTable[] {
    const id = this.idFunction(source);
    return this.cache.get(id) ?? [];
  }

  delete(source: T | SourceTable): boolean {
    const id = this.idFunction(source);
    return this.cache.delete(id);
  }
}
