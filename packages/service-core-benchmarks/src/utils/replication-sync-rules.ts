import { BenchmarkSourceTable } from '../types/BenchmarkScenario.js';

export function createStorageSyncRules(sourceTable: BenchmarkSourceTable): string {
  return `
bucket_definitions:
  global:
    data:
      - SELECT id, owner_id, category, version, updated_at, payload FROM ${qualifiedTable(sourceTable)}
`;
}

export function createReplicationSyncRules(sourceTable: BenchmarkSourceTable): string {
  return `
bucket_definitions:
  global:
    data:
      - SELECT id, owner_id, category, version, updated_at, payload, is_target FROM ${qualifiedTable(sourceTable)}
`;
}

export function createCategoryStorageSyncRules(sourceTable: BenchmarkSourceTable): string {
  return categorySyncRules(sourceTable, 'id, owner_id, category, version, updated_at, payload');
}

export function createCategoryReplicationSyncRules(sourceTable: BenchmarkSourceTable): string {
  return categorySyncRules(sourceTable, 'id, owner_id, category, version, updated_at, payload, is_target');
}

export function createCategorySyncParameters(): Record<string, unknown> {
  return { categories: Array.from({ length: 10 }, (_, index) => `category-${index}`) };
}

function categorySyncRules(sourceTable: BenchmarkSourceTable, columns: string): string {
  return `
bucket_definitions:
  category:
    accept_potentially_dangerous_queries: true
    parameters: SELECT value AS category FROM json_each(request.parameter('categories'))
    data:
      - SELECT ${columns} FROM ${qualifiedTable(sourceTable)} WHERE category = bucket.category
`;
}

export function qualifiedTable(sourceTable: BenchmarkSourceTable): string {
  return `${quoteIdentifier(sourceTable.schema)}.${quoteIdentifier(sourceTable.table)}`;
}

export function quoteIdentifier(identifier: string): string {
  return `"${identifier.replaceAll('"', '""')}"`;
}
