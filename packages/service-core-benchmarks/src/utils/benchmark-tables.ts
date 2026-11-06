import { storage } from '@powersync/service-core';
import type { BenchmarkDataDefinition } from '../types/BenchmarkDataDefinition.js';

export async function resolveBenchmarkTables(
  writer: storage.BucketStorageBatch,
  data: BenchmarkDataDefinition
): Promise<ReadonlyMap<string, storage.SourceTable>> {
  const tables = new Map<string, storage.SourceTable>();
  for (const table of data.tables) {
    const resolved = await writer.resolveTables({
      connection_id: 1,
      source: {
        connectionTag: storage.SourceTable.DEFAULT_TAG,
        objectId: table.name,
        schema: 'public',
        name: table.name,
        replicaIdColumns: [{ name: 'id', type: 'VARCHAR', typeId: 25 }]
      }
    });
    if (resolved.tables.length !== 1) throw new Error(`Expected one source table for ${table.key}`);
    tables.set(table.key, resolved.tables[0]);
  }
  return tables;
}
