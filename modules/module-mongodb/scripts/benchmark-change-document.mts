import * as bson from 'bson';
import { performance } from 'node:perf_hooks';
import { parseChangeDocument } from '../dist/index.js';

// This is a synthetic benchmark to test performance of parseChangeDocument
// versus the normal bson.deserialize().
// Primarily AI-generated.

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
  payload: string;
};

type UpdateDescription = {
  updatedFields: {
    payload: string;
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
  buffer: Buffer;
};

type BenchmarkResult = {
  elapsedMs: number;
  opsPerSecond: number;
  mibPerSecond: number;
};

const BENCHMARKS: readonly Benchmark[] = [
  {
    label: 'parseChangeDocument',
    run: (buffer: Buffer) => {
      const change = parseChangeDocument(buffer);
      if (!('fullDocument' in change)) {
        throw new Error('Unsupported change type: ' + change.operationType);
      }
      return change.fullDocument?.byteLength ?? 0;
    }
  },
  {
    label: 'parseChangeDocument + fullDocument',
    run: (buffer: Buffer) => {
      const change = parseChangeDocument(buffer);
      if (!('fullDocument' in change)) {
        throw new Error('Unsupported change type: ' + change.operationType);
      }
      if (change.fullDocument == null) {
        throw new Error('Expected fullDocument to be present');
      }
      const fullDocument = bson.deserialize(change.fullDocument, BSON_OPTIONS) as { checksum?: number };
      return Number(fullDocument.checksum ?? 0);
    }
  },
  {
    label: 'bson.deserialize',
    run: (buffer: Buffer) => {
      const change = bson.deserialize(buffer, BSON_OPTIONS) as { fullDocument?: { checksum?: number } };
      return Number(change.fullDocument?.checksum ?? 0);
    }
  }
] as const;

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
    payload: ''
  };

  const payloadLength = findPayloadLength(baseDocument, targetBytes);
  const payload = repeatCharacter('x', payloadLength);
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
    payload: repeatCharacter('x', payloadLength)
  });
}

function repeatCharacter(character: string, count: number): string {
  return character.repeat(Math.max(0, count));
}

function chooseIterations(eventBytes: number): number {
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
  const buffer = Buffer.from(bson.serialize(changeDocument));
  return {
    label: `${operationType} ${sizeLabel}`,
    operationType,
    targetBytes,
    fullDocumentBytes: bson.calculateObjectSize(fullDocument),
    eventBytes: buffer.byteLength,
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

function printHeader() {
  console.log('Comparing parseChangeDocument against plain bson.deserialize');
  console.log('The key comparison is "parseChangeDocument + fullDocument" vs "bson.deserialize".');
  console.log('');
}

function printRow(values: string[]): void {
  const widths = [16, 12, 12, 34, 16, 16];
  const line = values
    .map((value, index) => value.padEnd(widths[index] ?? value.length))
    .join('  ')
    .trimEnd();
  console.log(line);
}

printHeader();
printRow(['Scenario', 'Full doc', 'Event', 'Benchmark', 'Ops/s', 'MiB/s']);
printRow(['--------', '--------', '-----', '---------', '-----', '-----']);

for (const operationType of OPERATION_TYPES) {
  for (const size of SIZE_TARGETS) {
    const scenario = buildScenario(operationType, size.label, size.bytes);
    const iterations = chooseIterations(scenario.eventBytes);
    const results = BENCHMARKS.map((benchmark) => ({
      label: benchmark.label,
      ...runBenchmark(benchmark.label, benchmark.run, scenario.buffer, iterations)
    }));

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
