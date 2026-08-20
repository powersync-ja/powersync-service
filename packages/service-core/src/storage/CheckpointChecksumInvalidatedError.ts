import { InternalOpId } from '../util/util-index.js';
import { CheckpointInvalidatedError } from './CheckpointInvalidatedError.js';

/**
 * A checkpoint cannot be served because compaction rewrote a bucket-data
 * document across its end boundary.
 *
 * The sync loop must skip this checkpoint before it sends its checkpoint line.
 */
export class CheckpointChecksumInvalidatedError extends CheckpointInvalidatedError {
  constructor(
    checkpoint: InternalOpId,
    public readonly bucket: string
  ) {
    super(checkpoint, `Checkpoint ${checkpoint} was invalidated by compaction in bucket ${bucket}`);
    this.name = 'CheckpointChecksumInvalidatedError';
  }

  get logMetadata(): Record<string, unknown> {
    return { reason: 'compacted_before_checkpoint_line', bucket: this.bucket };
  }
}
