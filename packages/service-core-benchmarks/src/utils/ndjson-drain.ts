export interface NdjsonDrainObservation {
  readonly status: number;
  readonly headers: Record<string, string>;
  readonly lines: readonly unknown[];
  readonly wireBytes: number;
  readonly firstByteAtNs: string | null;
  readonly completedCheckpoint: string | null;
}

export async function drainNdjsonResponse(response: Response): Promise<NdjsonDrainObservation> {
  if (response.body == null) {
    throw new Error('API response did not include a body');
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  const lines: unknown[] = [];
  let pending = '';
  let wireBytes = 0;
  let firstByteAtNs: string | null = null;
  let completedCheckpoint: string | null = null;

  while (true) {
    const chunk = await reader.read();
    if (chunk.done) break;

    if (firstByteAtNs == null) {
      firstByteAtNs = process.hrtime.bigint().toString();
    }

    wireBytes += chunk.value.byteLength;
    pending += decoder.decode(chunk.value, { stream: true });
    const result = consumeLines(pending, lines);
    pending = result.pending;
    completedCheckpoint ??= result.completedCheckpoint;
    if (completedCheckpoint != null) {
      await reader.cancel();
      pending = '';
      break;
    }
  }

  if (completedCheckpoint == null) {
    pending += decoder.decode();
    const result = consumeLines(pending, lines);
    completedCheckpoint ??= result.completedCheckpoint;

    if (result.pending.length > 0) {
      throw new Error('API response ended with an incomplete NDJSON line');
    }
  }

  return {
    status: response.status,
    headers: Object.fromEntries(response.headers.entries()),
    lines,
    wireBytes,
    firstByteAtNs,
    completedCheckpoint
  };
}

function consumeLines(
  input: string,
  target: unknown[]
): { readonly pending: string; readonly completedCheckpoint: string | null } {
  const lines = input.split('\n');
  const pending = lines.pop() ?? '';
  let completedCheckpoint: string | null = null;
  for (const line of lines) {
    if (line.length === 0) continue;

    const value: unknown = JSON.parse(line);
    target.push(value);
    if (isCheckpointComplete(value)) {
      completedCheckpoint = value.checkpoint_complete.last_op_id;
    }
  }

  return { pending, completedCheckpoint };
}

function isCheckpointComplete(value: unknown): value is { checkpoint_complete: { last_op_id: string } } {
  if (value == null || typeof value !== 'object' || !('checkpoint_complete' in value)) return false;

  const completion = value.checkpoint_complete;
  return (
    completion != null &&
    typeof completion === 'object' &&
    'last_op_id' in completion &&
    typeof completion.last_op_id === 'string'
  );
}
