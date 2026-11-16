import { BucketDataDoc } from '../common/BucketDataDoc.js';

export const DEFAULT_MAX_DOC_SIZE_BYTES = 1024 * 1024; // 1MB

/**
 * Split an array of bucket data operations into chunks where each chunk's
 * total data size stays close to `maxDocSizeBytes`.
 *
 * The threshold is a target, not a hard ceiling — a single operation whose
 * `data` field exceeds the limit is placed in its own chunk and is NOT split.
 */
export function chunkBucketData(
  operations: BucketDataDoc[],
  maxDocSizeBytes = DEFAULT_MAX_DOC_SIZE_BYTES
): BucketDataDoc[][] {
  const chunks: BucketDataDoc[][] = [];
  let currentChunk: BucketDataDoc[] = [];
  let currentSize = 0;

  for (const op of operations) {
    // Chunk boundaries only need an estimate, but should be close to actual values.
    // This estimate is more CPU-efficient than calculating the actual BSON size.
    const opSize =
      192 +
      Buffer.byteLength(op.data ?? '') +
      2 * Buffer.byteLength(op.subkey ?? '') +
      Buffer.byteLength(op.table ?? '') +
      Buffer.byteLength(op.row_id ?? '');

    if (currentSize + opSize > maxDocSizeBytes && currentChunk.length > 0) {
      chunks.push(currentChunk);
      currentChunk = [];
      currentSize = 0;
    }

    currentChunk.push(op);
    currentSize += opSize;
  }

  if (currentChunk.length > 0) {
    chunks.push(currentChunk);
  }

  return chunks;
}
