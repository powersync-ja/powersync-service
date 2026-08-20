import { InternalOpId } from '../util/util-index.js';
import { CheckpointInvalidatedError } from './CheckpointInvalidatedError.js';

/**
 * A checkpoint cannot be served because parameter compaction has advanced past it, so the parameter
 * history needed to evaluate parameter queries at that checkpoint may be incomplete.
 *
 * Storage implementations that evaluate parameter queries in a snapshot pinned to the checkpoint
 * (MongoDB) never raise this: the snapshot still sees entries a later compaction pass removed.
 * Postgres storage has no pinned snapshot, so it compares the checkpoint against the compaction
 * boundary instead.
 *
 * The sync loop must skip this checkpoint before it sends its checkpoint line. The next checkpoint
 * is at or above the boundary, so it serves normally.
 */
export class CheckpointParametersInvalidatedError extends CheckpointInvalidatedError {
  constructor(
    checkpoint: InternalOpId,
    /** Parameter history below this boundary may have been compacted away. */
    public readonly invalidBefore: InternalOpId
  ) {
    super(
      checkpoint,
      `Checkpoint ${checkpoint} is below the parameter compaction boundary ${invalidBefore}, ` +
        `so parameter queries cannot be evaluated at it`
    );
    this.name = 'CheckpointParametersInvalidatedError';
  }

  get logMetadata(): Record<string, unknown> {
    return { reason: 'parameters_compacted_before_checkpoint', invalid_before: this.invalidBefore };
  }
}
