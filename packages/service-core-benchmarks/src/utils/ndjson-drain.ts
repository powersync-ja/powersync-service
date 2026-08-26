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
  readonly lines: readonly unknown[];
  readonly wireBytes: number;
  readonly firstByteAtNs: string | null;
  readonly completedCheckpoint: string | null;
  readonly checkpointLastOpId: string | null;
  readonly operations: readonly NdjsonDataOperation[];
  readonly bucketNames: readonly string[];
  readonly dataBucketNames: readonly string[];
  readonly checkpointLineIndex: number | null;
  readonly firstDataLineIndex: number | null;
  readonly completionLineIndex: number | null;
  readonly completedAtNs: string | null;
}

interface NdjsonDrainState {
  readonly lines: unknown[];
  readonly operations: NdjsonDataOperation[];
  readonly bucketNames: Set<string>;
  readonly dataBucketNames: Set<string>;
  completedCheckpoint: string | null;
  checkpointLastOpId: string | null;
  checkpointLineIndex: number | null;
  firstDataLineIndex: number | null;
  completionLineIndex: number | null;
}

export async function drainNdjsonResponse(response: Response): Promise<NdjsonDrainObservation> {
  if (response.body == null) {
    throw new Error('API response did not include a body');
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  const state: NdjsonDrainState = {
    lines: [],
    operations: [],
    bucketNames: new Set(),
    dataBucketNames: new Set(),
    completedCheckpoint: null,
    checkpointLastOpId: null,
    checkpointLineIndex: null,
    firstDataLineIndex: null,
    completionLineIndex: null
  };
  let pending = '';
  let wireBytes = 0;
  let firstByteAtNs: string | null = null;
  let completedAtNs: string | null = null;
  let cancellationAttempted = false;
  let primaryError: unknown;

  try {
    while (true) {
      const chunk = await reader.read();
      if (chunk.done) break;

      if (firstByteAtNs == null) {
        firstByteAtNs = process.hrtime.bigint().toString();
      }

      wireBytes += chunk.value.byteLength;
      pending += decoder.decode(chunk.value, { stream: true });
      pending = consumeLines(pending, state);
      if (state.completedCheckpoint != null) {
        completedAtNs = process.hrtime.bigint().toString();
        cancellationAttempted = true;
        await reader.cancel();
        pending = '';
        break;
      }
    }

    if (state.completedCheckpoint == null) {
      pending += decoder.decode();
      pending = consumeLines(pending, state);

      if (pending.length > 0) {
        throw new Error('API response ended with an incomplete NDJSON line');
      }
      if (state.completedCheckpoint == null) {
        throw new Error('API response ended before checkpoint_complete');
      }
      completedAtNs = process.hrtime.bigint().toString();
    }

    return {
      status: response.status,
      headers: Object.fromEntries(response.headers.entries()),
      lines: state.lines,
      wireBytes,
      firstByteAtNs,
      completedCheckpoint: state.completedCheckpoint,
      checkpointLastOpId: state.checkpointLastOpId,
      operations: state.operations,
      bucketNames: [...state.bucketNames].sort(),
      dataBucketNames: [...state.dataBucketNames].sort(),
      checkpointLineIndex: state.checkpointLineIndex,
      firstDataLineIndex: state.firstDataLineIndex,
      completionLineIndex: state.completionLineIndex,
      completedAtNs
    };
  } catch (error) {
    primaryError = error;
    if (!cancellationAttempted) {
      cancellationAttempted = true;
      try {
        await reader.cancel();
      } catch {}
    }
    throw error;
  } finally {
    try {
      reader.releaseLock();
    } catch (error) {
      if (primaryError == null) {
        throw error;
      }
    }
  }
}

function consumeLines(input: string, state: NdjsonDrainState): string {
  const lines = input.split('\n');
  const pending = lines.pop() ?? '';
  for (const line of lines) {
    if (line.length === 0) continue;

    const value: unknown = JSON.parse(line);
    const lineIndex = state.lines.length;
    state.lines.push(value);

    const checkpointLastOpId = readCheckpointLastOpId(value);
    if (checkpointLastOpId != null) {
      state.checkpointLastOpId = checkpointLastOpId;
      state.checkpointLineIndex ??= lineIndex;
    }

    updateBucketNames(value, state);

    const operations = readDataOperations(value);
    if (operations != null) {
      state.firstDataLineIndex ??= lineIndex;
      state.operations.push(...operations);
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
