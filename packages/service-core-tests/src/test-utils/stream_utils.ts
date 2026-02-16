import { utils } from '@powersync/service-core';
import { JSONBig } from '@powersync/service-jsonbig';

export function putOp(table: string, data: Record<string, any>): Partial<utils.OplogEntry> {
  return {
    op: 'PUT',
    object_type: table,
    object_id: String(data.id), // Object ids are always converted to strings
    data: JSONBig.stringify(data)
  };
}

export function removeOp(table: string, id: string): Partial<utils.OplogEntry> {
  return {
    op: 'REMOVE',
    object_type: table,
    object_id: String(id)
  };
}

export function compareIds(a: utils.OplogEntry, b: utils.OplogEntry) {
  return a.object_id!.localeCompare(b.object_id!);
}

export async function oneFromAsync<T>(source: Iterable<T> | AsyncIterable<T>): Promise<T> {
  const items: T[] = [];
  for await (const item of source) {
    items.push(item);
  }
  if (items.length != 1) {
    throw new Error(`One item expected, got: ${items.length}`);
  }
  return items[0];
}

export async function fromAsync<T>(source: Iterable<T> | AsyncIterable<T>): Promise<T[]> {
  const items: T[] = [];
  for await (const item of source) {
    items.push(item);
  }
  return items;
}
