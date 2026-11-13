import type {
  BenchmarkDataDefinition,
  BenchmarkDataPhase,
  BenchmarkTableDefinition
} from '../types/BenchmarkDataDefinition.js';

export interface BenchmarkRow {
  readonly [column: string]: string | number;
  readonly id: string;
  readonly owner_id: string;
  readonly category: string;
  readonly partition_key: number;
  readonly table_key: string;
  readonly version: number;
  readonly updated_at: string;
  readonly payload: string;
  readonly is_target: number;
}

export interface BenchmarkRowRange {
  readonly table: BenchmarkTableDefinition;
  readonly phase: BenchmarkDataPhase;
  readonly start: number;
  readonly count: number;
  readonly markLast: boolean;
}

export interface BenchmarkTransaction {
  readonly id: string;
  readonly position: string;
  readonly ranges: readonly BenchmarkRowRange[];
  readonly target: { readonly markerId: string; readonly nativePosition: string | null };
}

/** Restartable indexable sequence; retains its descriptor, never its generated items. */
export class BenchmarkTransactions implements Iterable<BenchmarkTransaction> {
  constructor(readonly data: BenchmarkDataDefinition) {}
  get length(): number {
    return this.data.transaction_count;
  }
  at(index: number): BenchmarkTransaction | undefined {
    if (!Number.isSafeInteger(index)) return undefined;
    if (index < 0) index += this.length;
    if (index < 0 || index >= this.length) return undefined;
    const ranges = this.data.tables.map((table) => {
      const start = Number((BigInt(index) * BigInt(table.streaming_row_count)) / BigInt(this.length));
      const end = Number((BigInt(index + 1) * BigInt(table.streaming_row_count)) / BigInt(this.length));
      return { table, phase: 'streaming' as const, start, count: end - start, markLast: true };
    });
    const last = ranges.at(-1)!;
    return {
      id: `transaction-${index}`,
      position: (index + 2).toString().padStart(20, '0'),
      ranges,
      target: { markerId: rowId(last.table, last.phase, last.start + last.count - 1), nativePosition: null }
    };
  }
  byId(id: string): BenchmarkTransaction | undefined {
    if (!/^transaction-(0|[1-9][0-9]*)$/.test(id)) return undefined;
    return this.at(Number(id.slice('transaction-'.length)));
  }
  find(predicate: (transaction: BenchmarkTransaction) => boolean): BenchmarkTransaction | undefined {
    for (const item of this) if (predicate(item)) return item;
    return undefined;
  }
  *[Symbol.iterator](): Iterator<BenchmarkTransaction> {
    for (let i = 0; i < this.length; i++) yield this.at(i)!;
  }
}

export interface BenchmarkDataManifest {
  readonly data: BenchmarkDataDefinition;
  readonly phase: BenchmarkDataPhase;
  readonly snapshot: readonly BenchmarkRowRange[];
  readonly transactions: BenchmarkTransactions;
  readonly target: { readonly markerId: string; readonly nativePosition: string | null };
  readonly sourceLogicalBytes: number;
  readonly payloadBytes: number;
  readonly snapshotRowCount: number;
  readonly streamingRowCount: number;
  readonly expectedPutCount: number;
}

export function rowId(table: BenchmarkTableDefinition, phase: BenchmarkDataPhase, index: number): string {
  return `${table.key}:${phase}:${index}`;
}

function fixedRow(range: BenchmarkRowRange, index: number): BenchmarkRow {
  return {
    id: rowId(range.table, range.phase, index),
    owner_id: `owner-${index % range.table.distribution.owner_count}`,
    category: `category-${index}`,
    partition_key: index,
    table_key: range.table.key,
    version: 1,
    updated_at: '2020-01-01T00:00:00.000000Z',
    payload: '',
    is_target: range.markLast && index === range.start + range.count - 1 ? 1 : 0
  };
}

export function maxFixedRowBytes(table: BenchmarkTableDefinition, phase: BenchmarkDataPhase): number {
  const count = phase === 'snapshot' ? table.snapshot_row_count : table.streaming_row_count;
  const range = { table, phase, start: 0, count, markLast: true };
  const owners = table.distribution.owner_count;
  let maximum = 0;
  // Within one index digit width, only owner-id digit width varies. Sample its maximum.
  for (let first = 0, next = 10; first < count; first = next, next *= 10) {
    const last = Math.min(count, next) - 1;
    const owner = Math.floor(first / owners) !== Math.floor(last / owners) ? owners - 1 : last % owners;
    const index = last - (((last % owners) - owner + owners) % owners);
    maximum = Math.max(maximum, Buffer.byteLength(JSON.stringify(fixedRow(range, index))));
  }
  return maximum;
}

export function createRow(range: BenchmarkRowRange, index: number): BenchmarkRow {
  const row = fixedRow(range, index);
  const remaining = range.table.target_source_json_bytes - Buffer.byteLength(JSON.stringify(row), 'utf8');
  if (remaining < 0) throw new RangeError(`target_source_json_bytes is smaller than fixed row bytes for ${row.id}`);
  return { ...row, payload: 'x'.repeat(remaining) };
}

export function* rows(range: BenchmarkRowRange, offset = 0, limit = range.count - offset): Iterable<BenchmarkRow> {
  if (!Number.isSafeInteger(offset) || offset < 0 || offset > range.count || !Number.isSafeInteger(limit) || limit < 0)
    throw new RangeError('Invalid row offset or limit');
  const end = range.start + Math.min(range.count, offset + limit);
  for (let index = range.start + offset; index < end; index++) yield createRow(range, index);
}

export function* rowBatches(range: BenchmarkRowRange, batchSize: number): Iterable<BenchmarkRow[]> {
  if (!Number.isSafeInteger(batchSize) || batchSize < 1) throw new RangeError('Invalid batch size');
  for (let offset = 0; offset < range.count; offset += batchSize) yield [...rows(range, offset, batchSize)];
}

export function createDataManifest(data: BenchmarkDataDefinition, phase: BenchmarkDataPhase): BenchmarkDataManifest {
  const snapshot = data.tables.map((table) => ({
    table,
    phase: 'snapshot' as const,
    start: 0,
    count: table.snapshot_row_count,
    markLast: true
  }));
  const transactions = new BenchmarkTransactions(data);
  const last = snapshot.at(-1)!;
  const target =
    phase === 'snapshot'
      ? { markerId: rowId(last.table, last.phase, last.count - 1), nativePosition: null }
      : transactions.at(-1)!.target;
  const measured = data.tables.map((table) => ({
    table,
    phase,
    start: 0,
    count: phase === 'snapshot' ? table.snapshot_row_count : table.streaming_row_count,
    markLast: true
  }));
  const snapshotRowCount = snapshot.reduce((sum, r) => sum + r.count, 0);
  const streamingRowCount = data.tables.reduce((sum, t) => sum + t.streaming_row_count, 0);
  return {
    data,
    phase,
    snapshot,
    transactions,
    target,
    sourceLogicalBytes: measured.reduce((sum, r) => sum + r.count * r.table.target_source_json_bytes, 0),
    payloadBytes: measured.reduce((sum, r) => sum + payloadBytes(r), 0),
    snapshotRowCount,
    streamingRowCount,
    expectedPutCount: snapshotRowCount + (phase === 'streaming' ? streamingRowCount : 0)
  };
}

/** Sum digit widths without walking the dataset. All variable row overhead is decimal ASCII. */
function digitBytes(count: number): number {
  let sum = 0;
  for (let start = 0, width = 1, end = 10; start < count; start = end, width++, end *= 10) {
    sum += (Math.min(count, end) - start) * width;
  }
  return sum;
}

function payloadBytes(range: BenchmarkRowRange): number {
  const emptyRow = { ...createRow(range, 0), payload: '' };
  const base = Buffer.byteLength(JSON.stringify(emptyRow)) - 4; // index appears in id, category, partition_key and owner_id
  const owners = range.table.distribution.owner_count;
  const ownerDigits = Math.floor(range.count / owners) * digitBytes(owners) + digitBytes(range.count % owners);
  return range.count * (range.table.target_source_json_bytes - base) - 3 * digitBytes(range.count) - ownerDigits;
}
