import { InternalOpId } from '../util/util-index.js';

/**
 * A checkpoint cannot be served, because compaction removed data that serving it would need.
 *
 * The sync loop must drop the checkpoint before sending its checkpoint line, and continue with the
 * next one. `BucketChecksumState.buildNextCheckpointLine()` advances no connection state until the
 * line is sent, so dropping a candidate is safe.
 */
export abstract class CheckpointInvalidatedError extends Error {
  constructor(
    public readonly checkpoint: InternalOpId,
    message: string
  ) {
    super(message);
  }

  /** Additional fields for the `checkpoint_invalidated` log entry. */
  abstract get logMetadata(): Record<string, unknown>;
}
