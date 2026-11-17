import { modules } from '@powersync/service-core';
import { CompatibilityContext } from '@powersync/service-sync-rules';
import * as bson from 'bson';
import { performance } from 'node:perf_hooks';
import { parseArgs } from 'node:util';
import {
  DirectSourceRowConverter,
  LegacySourceRowConverter,
  RustSourceRowConverter,
  parseChangeDocument
} from '../dist/index.js';

// This is a synthetic benchmark to test performance of parseChangeDocument
// versus the normal bson.deserialize().
// Primarily AI-generated.

const { values: options } = parseArgs({
  options: {
    'rows-only': { type: 'boolean', default: false },
    payload: { type: 'string', default: 'ascii' },
    iterations: { type: 'string' }
  }
});
if (!['ascii', 'unicode', 'nested', 'objects'].includes(options.payload)) {
  throw new Error('--payload must be ascii, unicode, nested, or objects');
}
const fixedIterations = options.iterations == null ? undefined : Number(options.iterations);
if (fixedIterations != null && (!Number.isSafeInteger(fixedIterations) || fixedIterations < 1)) {
  throw new Error('--iterations must be a positive integer');
}

const BSON_OPTIONS = { useBigInt64: true } as const;
const OPERATION_TYPES = ['insert', 'update'] as const;
const SIZE_TARGETS = [
  { label: '1 KB', bytes: 1_024 },
  { label: '10 KB', bytes: 10_240 },
  { label: '100 KB', bytes: 102_400 }
] as const;

type OperationType = (typeof OPERATION_TYPES)[number];

type FullDocument = {
  _id: bson.ObjectId;
  checksum: number;
  operationType: OperationType;
  tenantId: string;
  version: number;
  createdAt: Date;
  updatedAt: Date;
  flags: {
    active: boolean;
    archived: boolean;
    source: string;
  };
  metrics: {
    itemCount: number;
    ratio: number;
  };
  tags: string[];
  nested: {
    owner: {
      id: string;
      region: string;
    };
    counters: number[];
    changedFields: string[];
  };
  payload: string | { nested: string } | { label: string; value: number; active: boolean }[];
};

type UpdateDescription = {
  updatedFields: {
    payload: FullDocument['payload'];
    metrics: FullDocument['metrics'];
    updatedAt: Date;
    version: number;
  };
  removedFields: string[];
  truncatedArrays: never[];
};

type ChangeDocument = {
  _id: {
    _data: string;
  };
  operationType: OperationType;
  wallTime: Date;
  ns: {
    db: string;
    coll: string;
  };
  lsid: {
    id: bson.Binary;
  };
  txnNumber: bson.Long;
  documentKey: {
    _id: bson.ObjectId;
  };
  fullDocument: FullDocument;
  updateDescription?: UpdateDescription;
};

type Benchmark = {
  label: string;
  run: (buffer: Buffer) => number;
};

type Scenario = {
  label: string;
  operationType: OperationType;
  targetBytes: number;
  fullDocumentBytes: number;
  eventBytes: number;
  checksum: number;
  buffer: Buffer;
};

type BenchmarkResult = {
  elapsedMs: number;
  opsPerSecond: number;
  mibPerSecond: number;
};

const rawConverter = new DirectSourceRowConverter(CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY);
const docConverter = new LegacySourceRowConverter(CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY);

function fullDocumentBuffer(buffer: Buffer): Buffer {
  if (options['rows-only']) {
    return buffer;
  }
  const change = parseChangeDocument(buffer);
  if (!('fullDocument' in change) || change.fullDocument == null) {
    throw new Error('Expected fullDocument to be present');
  }
  return change.fullDocument;
}

const BENCHMARKS: Benchmark[] = [
  {
    label: options['rows-only'] ? 'rawToSqliteRow' : 'parseChangeDocument + rawToSqliteRow',
    run: (buffer: Buffer) => {
      const { row } = rawConverter.rawToSqliteRow(fullDocumentBuffer(buffer));
      return Number(row.checksum ?? 0);
    }
  },
  {
    label: 'bson.deserialize + documentToSqliteRow',
    run: (buffer: Buffer) => {
      const change = bson.deserialize(buffer, BSON_OPTIONS) as { fullDocument?: { checksum?: number } };
      const fullDocument = docConverter.documentToSqliteRow(options['rows-only'] ? change : change.fullDocument!);
      return Number(fullDocument.checksum ?? 0);
    }
  }
];

// Build a release addon first and set POWERSYNC_RUST_ADDON to include Rust in the comparison.
if (process.env.POWERSYNC_RUST_ADDON) {
  const rustModule = new modules.RustModule({ name: 'Rust', addonPath: process.env.POWERSYNC_RUST_ADDON });
  await rustModule.loadRust();
  const rustConverter = new RustSourceRowConverter(CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY, rustModule);
  BENCHMARKS.push({
    label: options['rows-only'] ? 'Rust rawToSqliteRow' : 'parseChangeDocument + Rust rawToSqliteRow',
    run: (buffer) => {
      return Number(rustConverter.rawToSqliteRow(fullDocumentBuffer(buffer)).row.checksum ?? 0);
    }
  });
}

function createFullDocument(operationType: OperationType, targetBytes: number): FullDocument {
  const seed = targetBytes + (operationType === 'insert' ? 11 : 29);
  const baseDocument: FullDocument = {
    _id: new bson.ObjectId(),
    checksum: seed,
    operationType,
    tenantId: 'tenant-benchmark',
    version: operationType === 'insert' ? 1 : 2,
    createdAt: new Date('2026-01-01T00:00:00.000Z'),
    updatedAt: new Date('2026-01-02T03:04:05.000Z'),
    flags: {
      active: true,
      archived: false,
      source: 'benchmark'
    },
    metrics: {
      itemCount: seed,
      ratio: Number((targetBytes / 1024).toFixed(3))
    },
    tags: ['alpha', 'beta', 'gamma', operationType],
    nested: {
      owner: {
        id: `owner-${seed}`,
        region: 'af-south-1'
      },
      counters: [1, 2, 3, 5, 8, 13],
      changedFields: operationType === 'update' ? ['payload', 'metrics.itemCount', 'updatedAt'] : []
    },
    payload: createPayload(0)
  };

  const payloadLength = findPayloadLength(baseDocument, targetBytes);
  const payload = createPayload(payloadLength);
  return {
    ...baseDocument,
    payload
  };
}

function createChangeDocument(fullDocument: FullDocument, operationType: OperationType): ChangeDocument {
  const updateDescription: UpdateDescription | undefined =
    operationType === 'update'
      ? {
          updatedFields: {
            payload: fullDocument.payload,
            metrics: fullDocument.metrics,
            updatedAt: fullDocument.updatedAt,
            version: fullDocument.version
          },
          removedFields: ['legacyField'],
          truncatedArrays: []
        }
      : undefined;

  return {
    _id: {
      _data: `${operationType}-${fullDocument.checksum}-${new bson.ObjectId().toHexString()}`
    },
    operationType,
    wallTime: new Date('2026-01-03T09:10:11.000Z'),
    ns: {
      db: 'benchmark_db',
      coll: 'benchmark_coll'
    },
    lsid: {
      id: new bson.Binary(Buffer.alloc(16, operationType === 'insert' ? 0x11 : 0x22))
    },
    txnNumber: seedLong(fullDocument.checksum),
    documentKey: {
      _id: fullDocument._id
    },
    ...(updateDescription == null ? {} : { updateDescription }),
    fullDocument
  };
}

function seedLong(value: number): bson.Long {
  return bson.Long.fromNumber(value);
}

function findPayloadLength(baseDocument: FullDocument, targetBytes: number): number {
  const baseSize = bson.calculateObjectSize(baseDocument);
  if (baseSize >= targetBytes) {
    return 0;
  }

  let low = 0;
  let high = Math.max(16, targetBytes - baseSize);
  while (calculateSizedDocumentBytes(baseDocument, high) < targetBytes) {
    high *= 2;
  }

  while (low < high) {
    const mid = Math.floor((low + high) / 2);
    if (calculateSizedDocumentBytes(baseDocument, mid) < targetBytes) {
      low = mid + 1;
    } else {
      high = mid;
    }
  }

  return low;
}

function calculateSizedDocumentBytes(baseDocument: FullDocument, payloadLength: number): number {
  return bson.calculateObjectSize({
    ...baseDocument,
    payload: createPayload(payloadLength)
  });
}

function createPayload(count: number): FullDocument['payload'] {
  switch (options.payload) {
    case 'unicode':
      return 'é☃🚀'.repeat(count);
    case 'nested':
      return { nested: 'x'.repeat(count) };
    case 'objects':
      return Array.from({ length: count }, (_, i) => ({ label: `item-${i}`, value: i, active: i % 2 === 0 }));
    default:
      return 'x'.repeat(count);
  }
}

function chooseIterations(eventBytes: number): number {
  if (fixedIterations != null) {
    return fixedIterations;
  }
  const targetBytes = 256 * 1024 * 1024;
  return clamp(Math.floor(targetBytes / eventBytes), 200, 50_000);
}

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

function median(values: number[]): number {
  const sorted = [...values].sort((a, b) => a - b);
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0 ? (sorted[middle - 1] + sorted[middle]) / 2 : sorted[middle];
}

function buildScenario(operationType: OperationType, sizeLabel: string, targetBytes: number): Scenario {
  const fullDocument = createFullDocument(operationType, targetBytes);
  const changeDocument = createChangeDocument(fullDocument, operationType);
  const buffer = Buffer.from(bson.serialize(options['rows-only'] ? fullDocument : changeDocument));
  return {
    label: `${operationType} ${sizeLabel}`,
    operationType,
    targetBytes,
    fullDocumentBytes: bson.calculateObjectSize(fullDocument),
    eventBytes: buffer.byteLength,
    checksum: fullDocument.checksum,
    buffer
  };
}

function runBenchmark(
  label: string,
  fn: (buffer: Buffer) => number,
  buffer: Buffer,
  iterations: number
): BenchmarkResult {
  const warmupIterations = Math.min(2_000, Math.max(50, Math.floor(iterations / 10)));
  let sink = 0;
  for (let i = 0; i < warmupIterations; i += 1) {
    sink += fn(buffer);
  }

  const samples: number[] = [];
  for (let round = 0; round < 5; round += 1) {
    const start = performance.now();
    for (let i = 0; i < iterations; i += 1) {
      sink += fn(buffer);
    }
    samples.push(performance.now() - start);
  }

  if (sink === Number.MIN_SAFE_INTEGER) {
    console.error(label);
  }

  const elapsedMs = median(samples);
  const opsPerSecond = (iterations * 1000) / elapsedMs;
  const mibPerSecond = (buffer.byteLength * iterations) / (1024 * 1024) / (elapsedMs / 1000);
  return {
    elapsedMs,
    opsPerSecond,
    mibPerSecond
  };
}

function printRow(values: string[]): void {
  const widths = [16, 10, 10, 42, 10, 10];
  const line = values
    .map((value, index) => value.padEnd(widths[index] ?? value.length))
    .join('  ')
    .trimEnd();
  console.log(line);
}

console.log(
  `Payload: ${options.payload}; ${options['rows-only'] ? 'row conversion only' : 'change event + row conversion'}`
);
printRow(['Scenario', 'Full doc', options['rows-only'] ? 'Input' : 'Event', 'Benchmark', 'Ops/s', 'MiB/s']);
printRow(['--------', '--------', '-----', '---------', '-----', '-----']);

for (const operationType of OPERATION_TYPES) {
  for (const size of SIZE_TARGETS) {
    const scenario = buildScenario(operationType, size.label, size.bytes);
    const iterations = chooseIterations(scenario.eventBytes);
    const results = BENCHMARKS.map((benchmark) => {
      if (benchmark.run(scenario.buffer) !== scenario.checksum) {
        throw new Error(`Unexpected checksum from ${benchmark.label}`);
      }
      return {
        label: benchmark.label,
        ...runBenchmark(benchmark.label, benchmark.run, scenario.buffer, iterations)
      };
    });

    let isFirstRow = true;
    for (const result of results) {
      printRow([
        isFirstRow ? scenario.label : '',
        isFirstRow ? formatBytes(scenario.fullDocumentBytes) : '',
        isFirstRow ? formatBytes(scenario.eventBytes) : '',
        result.label,
        formatNumber(result.opsPerSecond),
        formatNumber(result.mibPerSecond)
      ]);
      isFirstRow = false;
    }
  }
}

function formatNumber(value: number): string {
  return new Intl.NumberFormat('en-US', {
    maximumFractionDigits: value >= 100 ? 0 : 1
  }).format(value);
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) {
    return `${bytes} B`;
  }
  if (bytes < 1024 * 1024) {
    return `${(bytes / 1024).toFixed(bytes >= 10 * 1024 ? 0 : 1)} KB`;
  }
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}
