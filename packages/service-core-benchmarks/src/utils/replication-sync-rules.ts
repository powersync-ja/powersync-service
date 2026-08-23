import { ReplicationBenchmarkSourceTable } from '../types/ReplicationBenchmark.js';

export function createReplicationSyncRules(iterationId: string, sourceTable: ReplicationBenchmarkSourceTable): string {
  return `
# ${iterationId}
bucket_definitions:
  global:
    data:
      - SELECT id, owner_id, category, version, updated_at, payload, is_target FROM ${quoteIdentifier(sourceTable.schema)}.${quoteIdentifier(sourceTable.table)}
`;
}

function quoteIdentifier(identifier: string): string {
  return `"${identifier.replaceAll('"', '""')}"`;
}
