import { BenchmarkSourceTable } from '../types/BenchmarkScenario.js';

export function qualifiedTable(sourceTable: BenchmarkSourceTable): string {
  return `${quoteIdentifier(sourceTable.schema)}.${quoteIdentifier(sourceTable.table)}`;
}

export function quoteIdentifier(identifier: string): string {
  return `"${identifier.replaceAll('"', '""')}"`;
}
