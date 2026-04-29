export interface CheckpointStateInput {
  lsn: string;
  snapshotDone: boolean;
  lastCheckpointLsn: string | null;
  noCheckpointBefore: string | null;
  keepaliveOp: bigint | null;
  lastCheckpoint: bigint | null;
  persistedOp: bigint | null;
  createEmptyCheckpoints: boolean;
}

export interface CheckpointStateResult {
  canCheckpoint: boolean;
  checkpointBlocked: boolean;
  checkpointCreated: boolean;
  notEmpty: boolean;
  newKeepaliveOp: bigint | null;
  newLastCheckpoint: bigint | null;
}

function maxOpId(...values: (bigint | null | undefined)[]): bigint {
  let max = 0n;
  for (const value of values) {
    if (value != null && value > max) {
      max = value;
    }
  }
  return max;
}

export function canCheckpointState(
  lsn: string,
  state: Pick<CheckpointStateInput, 'snapshotDone' | 'lastCheckpointLsn' | 'noCheckpointBefore'>
): boolean {
  return (
    state.snapshotDone === true &&
    (state.lastCheckpointLsn == null || state.lastCheckpointLsn <= lsn) &&
    (state.noCheckpointBefore == null || state.noCheckpointBefore <= lsn)
  );
}

export function calculateCheckpointState(input: CheckpointStateInput): CheckpointStateResult {
  const canCheckpoint = canCheckpointState(input.lsn, input);
  const newKeepaliveOp = canCheckpoint ? null : maxOpId(input.keepaliveOp, input.persistedOp);
  const newLastCheckpoint = canCheckpoint
    ? maxOpId(input.lastCheckpoint, input.persistedOp, input.keepaliveOp)
    : input.lastCheckpoint;
  const notEmpty =
    input.createEmptyCheckpoints ||
    input.keepaliveOp !== newKeepaliveOp ||
    input.lastCheckpoint !== newLastCheckpoint;

  return {
    canCheckpoint,
    checkpointBlocked: !canCheckpoint,
    checkpointCreated: canCheckpoint && notEmpty,
    notEmpty,
    newKeepaliveOp,
    newLastCheckpoint
  };
}
