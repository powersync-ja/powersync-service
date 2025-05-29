import { SourceTable } from '../storage/SourceTable.js';

export class RelationCache<T> {
  private cache = new Map<string | number, SourceTable>();
  private idFunction: (item: T | SourceTable) => string | number;

  constructor(idFunction: (item: T | SourceTable) => string | number) {
    this.idFunction = idFunction;
  }

  update(table: SourceTable) {
    const id = this.idFunction(table);
    this.cache.set(id, table);
  }

  get(source: T | SourceTable): SourceTable | undefined {
    const id = this.idFunction(source);
    return this.cache.get(id);
  }

  delete(source: T | SourceTable): boolean {
    const id = this.idFunction(source);
    return this.cache.delete(id);
  }
}
