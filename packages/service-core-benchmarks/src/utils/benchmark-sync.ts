import type { RequestedStream } from '@powersync/service-sync-rules';
import { createRow, rowId } from '../generators/benchmark-data.js';
import type { BenchmarkDataDefinition, BenchmarkDataPhase } from '../types/BenchmarkDataDefinition.js';
import type { BenchmarkCorrectnessCheck } from '../types/BenchmarkIteration.js';
import type { BenchmarkSourceTable } from '../types/BenchmarkScenario.js';
import type { BenchmarkSyncDefinition } from '../types/BenchmarkSyncDefinition.js';
import { operationMatchesRow, streamNameForBucket, type OperationEvidence } from './operation-evidence.js';
import { qualifiedTable } from './replication-sync-rules.js';

export function renderBenchmarkSync(
  data: BenchmarkDataDefinition,
  sync: BenchmarkSyncDefinition,
  source: BenchmarkSourceTable
): string {
  const sections = sync.streams
    .map((stream) => {
      const queries = stream.tables
        .map((key) => {
          const table = data.tables.find((t) => t.key === key);
          if (table == null) throw new Error(`Missing sync table ${key}`);
          const qualified = qualifiedTable({ schema: source.schema, table: table.name });
          const where =
            stream.bucket_count === 1
              ? ''
              : ` WHERE (partition_key - CAST(partition_key / ${stream.bucket_count} AS INTEGER) * ${stream.bucket_count}) = ${sync.engine === 'sync-streams' ? "subscription.parameter('partition')" : 'bucket.partition'}`;
          return `      - ${JSON.stringify(`SELECT * FROM ${qualified}${where}`)}`;
        })
        .join('\n');
      if (sync.engine === 'sync-streams')
        return `  ${stream.name}:\n    ${stream.bucket_count === 1 ? 'auto_subscribe: true' : 'accept_potentially_dangerous_queries: true'}\n    queries:\n${queries}`;
      const parameters =
        stream.bucket_count === 1
          ? ''
          : `    accept_potentially_dangerous_queries: true\n    parameters: SELECT value AS partition FROM json_each(request.parameter('${stream.name}_partitions'))\n`;
      return `  ${stream.name}:\n${parameters}    data:\n${queries}`;
    })
    .join('\n');
  return sync.engine === 'sync-streams'
    ? `config:\n  edition: 3\nstreams:\n${sections}\n`
    : `bucket_definitions:\n${sections}\n`;
}

export function benchmarkClientParameters(sync: BenchmarkSyncDefinition): Record<string, unknown> {
  return sync.engine === 'sync-streams'
    ? {}
    : Object.fromEntries(
        sync.streams
          .filter((s) => s.bucket_count > 1)
          .map((s) => [`${s.name}_partitions`, Array.from({ length: s.bucket_count }, (_, index) => index)])
      );
}

export function benchmarkSubscriptions(sync: BenchmarkSyncDefinition): Record<string, RequestedStream[]> {
  return sync.engine === 'sync-rules'
    ? {}
    : Object.fromEntries(
        sync.streams
          .filter((s) => s.bucket_count > 1)
          .map((s) => [
            s.name,
            Array.from({ length: s.bucket_count }, (_, index) => ({
              priorityOverride: null,
              parameters: { partition: index },
              opaque_id: index
            }))
          ])
      );
}

export function benchmarkClientStreams(
  sync: BenchmarkSyncDefinition
): { readonly stream: string; readonly parameters: Record<string, number>; readonly override_priority: null }[] {
  return Object.entries(benchmarkSubscriptions(sync)).flatMap(([stream, subscriptions]) =>
    subscriptions.map((s) => ({ stream, parameters: s.parameters as Record<string, number>, override_priority: null }))
  );
}

export function benchmarkMarkerIds(data: BenchmarkDataDefinition): string[] {
  return data.tables.flatMap((table) =>
    (['snapshot', 'streaming'] as const).flatMap((phase) => {
      const count = phase === 'snapshot' ? table.snapshot_row_count : table.streaming_row_count;
      return [rowId(table, phase, 0), rowId(table, phase, count - 1)];
    })
  );
}

export function verifyBenchmarkEvidence(
  data: BenchmarkDataDefinition,
  sync: BenchmarkSyncDefinition,
  evidence: OperationEvidence,
  phases: readonly BenchmarkDataPhase[]
): BenchmarkCorrectnessCheck[] {
  const checks: BenchmarkCorrectnessCheck[] = [];
  const expectedTables: Record<string, number> = Object.create(null);
  const expectedStreams: Record<string, number> = Object.create(null);
  let expectedOperations = 0;
  for (const phase of phases) {
    const expected = expectedMembership(data, sync, phase);
    expectedOperations += expected.operations;
    for (const [key, count] of Object.entries(expected.tables))
      expectedTables[key] = (expectedTables[key] ?? 0) + count;
    for (const [key, count] of Object.entries(expected.streams))
      expectedStreams[key] = (expectedStreams[key] ?? 0) + count;
    checks.push({
      name: `phase_${phase}_operations`,
      passed: evidence.byPhase[phase] === expected.operations,
      details: { expected: expected.operations, actual: evidence.byPhase[phase] ?? 0 }
    });
    for (const table of data.tables) {
      const count = phase === 'snapshot' ? table.snapshot_row_count : table.streaming_row_count;
      const range = { table, phase, start: 0, count, markLast: true };
      for (const index of new Set([0, count - 1])) {
        const row = createRow(
          phase === 'streaming' && index === 0
            ? { ...range, count: Math.floor(count / data.transaction_count) }
            : range,
          index
        );
        const marker = evidence.markers[row.id];
        const memberships = sync.streams.filter((s) => s.tables.includes(table.key));
        const passed =
          marker != null &&
          marker.count === memberships.length &&
          memberships.every(
            (s) => marker.byStream[s.name] === 1 && operationMatchesRow(marker.operationsByStream[s.name], row)
          );
        checks.push({
          name: `sample_${row.id}`,
          passed,
          details: {
            expected_memberships: memberships.length,
            actual_memberships: marker?.count ?? 0,
            actual_sample: marker?.operation ?? null
          }
        });
      }
    }
  }
  for (const [kind, expected, actual] of [
    ['table', expectedTables, evidence.byTable],
    ['stream', expectedStreams, evidence.byStream]
  ] as const) {
    for (const key of new Set([...Object.keys(expected), ...Object.keys(actual)]))
      checks.push({
        name: `${kind}_${key}_operations`,
        passed: expected[key] === actual[key],
        details: { expected: expected[key] ?? 0, actual: actual[key] ?? 0 }
      });
  }
  checks.push({
    name: 'total_memberships',
    passed: evidence.operationCount === expectedOperations && evidence.putCount === expectedOperations,
    details: { expected: expectedOperations, actual: evidence.operationCount, puts: evidence.putCount }
  });
  for (const [bucket, actual] of Object.entries(evidence.byBucket)) {
    const name = streamNameForBucket(bucket);
    const definition = sync.streams.find((s) => s.name === name);
    const match = bucket.match(/\[([0-9]*)\]$/);
    const partition = match?.[1] ? Number(match[1]) : 0;
    const validPartition =
      definition != null &&
      match != null &&
      (definition.bucket_count === 1 ? match[1] === '' : match[1] !== '' && partition < definition.bucket_count);
    const expected = !validPartition
      ? -1
      : definition.tables.reduce((total, key) => {
          const table = data.tables.find((t) => t.key === key)!;
          return (
            total +
            phases.reduce((sum, phase) => {
              const count = phase === 'snapshot' ? table.snapshot_row_count : table.streaming_row_count;
              return (
                sum +
                Math.floor(count / definition.bucket_count) +
                (partition < count % definition.bucket_count ? 1 : 0)
              );
            }, 0)
          );
        }, 0);
    checks.push({ name: `bucket_${bucket}_operations`, passed: actual === expected, details: { expected, actual } });
  }
  return checks;
}

export function expectedMembership(
  data: BenchmarkDataDefinition,
  sync: BenchmarkSyncDefinition,
  phase: BenchmarkDataPhase
): {
  readonly operations: number;
  readonly tables: Record<string, number>;
  readonly streams: Record<string, number>;
} {
  const tables: Record<string, number> = Object.create(null);
  const streams: Record<string, number> = Object.create(null);
  let operations = 0;
  for (const stream of sync.streams) {
    streams[stream.name] = 0;
    for (const key of stream.tables) {
      const table = data.tables.find((t) => t.key === key)!;
      const count = phase === 'snapshot' ? table.snapshot_row_count : table.streaming_row_count;
      tables[key] = (tables[key] ?? 0) + count;
      streams[stream.name] += count;
      operations += count;
    }
  }
  return { operations, tables, streams };
}
