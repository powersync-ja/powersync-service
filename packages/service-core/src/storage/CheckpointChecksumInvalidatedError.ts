import { InternalOpId } from '../util/util-index.js';

/**
 * A checkpoint cannot be served because compaction rewrote a bucket-data
 * document across its end boundary.
 *
 * The sync loop must skip this checkpoint before it sends its checkpoint line.
 */
export class CheckpointChecksumInvalidatedError extends Error {
  constructor(
    public readonly checkpoint: InternalOpId,
    public readonly bucket: string
  ) {
    super(`Checkpoint ${checkpoint} was invalidated by compaction in bucket ${bucket}`);
    this.name = 'CheckpointChecksumInvalidatedError';
  }
}
