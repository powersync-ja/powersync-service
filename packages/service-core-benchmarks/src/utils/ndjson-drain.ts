import { OperationAccumulator, OperationEvidence } from './operation-evidence.js';

export interface NdjsonDataOperation {
  readonly [field: string]: unknown;
  readonly op_id: string;
  readonly op: string;
  readonly object_id?: string;
  readonly data?: unknown;
}

export interface NdjsonDrainObservation {
  readonly status: number;
  readonly headers: Record<string, string>;
  readonly lineCount: number;
  readonly wireBytes: number;
  readonly firstByteAtNs: string | null;
  readonly completedCheckpoint: string | null;
  readonly checkpointLastOpId: string | null;
  readonly evidence: OperationEvidence;
  readonly bucketNames: readonly string[];
  readonly dataBucketNames: readonly string[];
  readonly checkpointLineIndex: number | null;
  readonly firstDataLineIndex: number | null;
  readonly completionLineIndex: number | null;
  readonly completedAtNs: string | null;
}

interface NdjsonDrainState {
  lineCount: number;
  readonly accumulator: OperationAccumulator;
  readonly bucketNames: Set<string>;
  readonly dataBucketNames: Set<string>;
  completedCheckpoint: string | null;
  checkpointLastOpId: string | null;
  checkpointLineIndex: number | null;
  firstDataLineIndex: number | null;
  completionLineIndex: number | null;
}

export async function drainNdjsonResponse(
  response: Response,
  markerIds: readonly string[] = []
): Promise<NdjsonDrainObservation> {
  const reader = createNdjsonStreamReader(response, markerIds);
  try {
    return await reader.nextCheckpoint();
  } finally {
    await reader.close();
  }
}

export interface NdjsonStreamReader {
  nextCheckpoint(): Promise<NdjsonDrainObservation>;
  close(): Promise<void>;
}

export async function drainNdjsonUntilCheckpoint(
  reader: NdjsonStreamReader,
  targetCheckpoint: string
): Promise<NdjsonDrainObservation> {
  let lineCount = 0;
  const accumulator = new OperationAccumulator();
  const bucketNames = new Set<string>();
  const dataBucketNames = new Set<string>();
  let wireBytes = 0;
  let firstByteAtNs: string | null = null;
  let checkpointLineIndex: number | null = null;
  let firstDataLineIndex: number | null = null;
  let latest: NdjsonDrainObservation;
  do {
    latest = await reader.nextCheckpoint();
    const offset = lineCount;
    lineCount += latest.lineCount;
    accumulator.merge(latest.evidence);
    latest.bucketNames.forEach((name) => bucketNames.add(name));
    latest.dataBucketNames.forEach((name) => dataBucketNames.add(name));
    wireBytes += latest.wireBytes;
    firstByteAtNs ??= latest.firstByteAtNs;
    checkpointLineIndex ??= latest.checkpointLineIndex == null ? null : offset + latest.checkpointLineIndex;
    firstDataLineIndex ??= latest.firstDataLineIndex == null ? null : offset + latest.firstDataLineIndex;
  } while (latest.completedCheckpoint == null || BigInt(latest.completedCheckpoint) < BigInt(targetCheckpoint));
  return {
    ...latest,
    lineCount,
    evidence: accumulator.summary(),
    bucketNames: latest.bucketNames,
    dataBucketNames: [...dataBucketNames].sort(),
    wireBytes,
    firstByteAtNs,
    checkpointLineIndex,
    firstDataLineIndex,
    completionLineIndex: lineCount - 1
  };
}

export function createNdjsonStreamReader(response: Response, markerIds: readonly string[] = []): NdjsonStreamReader {
  if (response.body == null) {
    throw new Error('API response did not include a body');
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let pending = '';
  let closed = false;
  const bucketNames = new Set<string>();
  return {
    async nextCheckpoint() {
      if (closed) throw new Error('NDJSON stream reader is closed');
      const state = createDrainState(markerIds, bucketNames);
      let wireBytes = 0;
      let firstByteAtNs: string | null = null;
      while (state.completedCheckpoint == null) {
        const lineEnd = pending.indexOf('\n');
        if (lineEnd >= 0) {
          const line = pending.slice(0, lineEnd + 1);
          pending = pending.slice(lineEnd + 1);
          wireBytes += Buffer.byteLength(line);
          consumeLines(line, state);
          continue;
        }
        const chunk = await reader.read();
        if (chunk.done) {
          pending += decoder.decode();
          if (pending.length > 0) throw new Error('API response ended with an incomplete NDJSON line');
          throw new Error('API response ended before checkpoint_complete');
        }
        firstByteAtNs ??= process.hrtime.bigint().toString();
        pending += decoder.decode(chunk.value, { stream: true });
      }
      return {
        status: response.status,
        headers: Object.fromEntries(response.headers.entries()),
        lineCount: state.lineCount,
        wireBytes,
        firstByteAtNs,
        completedCheckpoint: state.completedCheckpoint,
        checkpointLastOpId: state.checkpointLastOpId,
        evidence: state.accumulator.summary(),
        bucketNames: [...state.bucketNames].sort(),
        dataBucketNames: [...state.dataBucketNames].sort(),
        checkpointLineIndex: state.checkpointLineIndex,
        firstDataLineIndex: state.firstDataLineIndex,
        completionLineIndex: state.completionLineIndex,
        completedAtNs: process.hrtime.bigint().toString()
      };
    },
    async close() {
      if (closed) return;
      closed = true;
      try {
        await reader.cancel();
      } finally {
        reader.releaseLock();
      }
    }
  };
}

function createDrainState(markerIds: readonly string[], bucketNames: Set<string>): NdjsonDrainState {
  return {
    lineCount: 0,
    accumulator: new OperationAccumulator(markerIds),
    bucketNames,
    dataBucketNames: new Set(),
    completedCheckpoint: null,
    checkpointLastOpId: null,
    checkpointLineIndex: null,
    firstDataLineIndex: null,
    completionLineIndex: null
  };
}

function consumeLines(input: string, state: NdjsonDrainState): string {
  const lines = input.split('\n');
  const pending = lines.pop() ?? '';
  for (const line of lines) {
    if (line.length === 0) continue;

    const value: unknown = JSON.parse(line);
    const lineIndex = state.lineCount++;

    const checkpointLastOpId = readCheckpointLastOpId(value);
    if (checkpointLastOpId != null) {
      state.checkpointLastOpId = checkpointLastOpId;
      state.checkpointLineIndex ??= lineIndex;
    }

    updateBucketNames(value, state);

    const operations = readDataOperations(value);
    if (operations != null) {
      state.firstDataLineIndex ??= lineIndex;
      const bucket = (value as { data: { bucket: string } }).data.bucket;
      for (const operation of operations) state.accumulator.add(bucket, operation);
    }

    const completedCheckpoint = readCompletedCheckpoint(value);
    if (completedCheckpoint != null) {
      state.completedCheckpoint = completedCheckpoint;
      state.completionLineIndex = lineIndex;
      break;
    }
  }

  return pending;
}

function updateBucketNames(value: unknown, state: NdjsonDrainState): void {
  if (!isRecord(value)) return;

  if ('checkpoint' in value) {
    if (!isRecord(value.checkpoint) || !Array.isArray(value.checkpoint.buckets)) {
      throw new Error('API response included a malformed NDJSON checkpoint bucket list');
    }

    state.bucketNames.clear();

    for (const bucket of value.checkpoint.buckets) {
      state.bucketNames.add(readBucketName(bucket));
    }
  }

  if ('checkpoint_diff' in value) {
    if (
      !isRecord(value.checkpoint_diff) ||
      !Array.isArray(value.checkpoint_diff.updated_buckets) ||
      !Array.isArray(value.checkpoint_diff.removed_buckets) ||
      !value.checkpoint_diff.removed_buckets.every((bucket) => typeof bucket === 'string')
    ) {
      throw new Error('API response included a malformed NDJSON checkpoint diff bucket list');
    }

    for (const bucket of value.checkpoint_diff.updated_buckets) {
      state.bucketNames.add(readBucketName(bucket));
    }

    for (const bucket of value.checkpoint_diff.removed_buckets) {
      state.bucketNames.delete(bucket);
    }
  }

  if ('data' in value) {
    if (!isRecord(value.data) || typeof value.data.bucket !== 'string') {
      throw new Error('API response included a malformed NDJSON data bucket');
    }

    state.dataBucketNames.add(value.data.bucket);
  }
}

function readBucketName(value: unknown): string {
  if (!isRecord(value) || typeof value.bucket !== 'string') {
    throw new Error('API response included a malformed NDJSON bucket');
  }
  return value.bucket;
}

function readCheckpointLastOpId(value: unknown): string | null {
  if (!isRecord(value)) return null;
  if ('checkpoint' in value) return readLastOpId(value.checkpoint, 'checkpoint');
  if ('checkpoint_diff' in value) return readLastOpId(value.checkpoint_diff, 'checkpoint_diff');
  return null;
}

function readDataOperations(value: unknown): readonly NdjsonDataOperation[] | null {
  if (!isRecord(value) || !('data' in value)) return null;
  if (!isRecord(value.data) || !Array.isArray(value.data.data) || !value.data.data.every(isDataOperation)) {
    throw new Error('API response included a malformed NDJSON data line');
  }
  return value.data.data;
}

function readCompletedCheckpoint(value: unknown): string | null {
  if (!isRecord(value) || !('checkpoint_complete' in value)) return null;
  return readLastOpId(value.checkpoint_complete, 'checkpoint_complete');
}

function readLastOpId(value: unknown, lineKind: string): string {
  if (!isRecord(value) || typeof value.last_op_id !== 'string') {
    throw new Error(`API response included a malformed NDJSON ${lineKind} line`);
  }
  return value.last_op_id;
}

function isDataOperation(value: unknown): value is NdjsonDataOperation {
  if (!isRecord(value) || typeof value.op_id !== 'string' || typeof value.op !== 'string') return false;
  if ('object_id' in value && typeof value.object_id !== 'string') return false;
  return true;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value != null && typeof value === 'object' && !Array.isArray(value);
}
