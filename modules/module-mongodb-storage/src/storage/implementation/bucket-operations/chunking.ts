import { BucketDataDoc } from '../common/BucketDataDoc.js';

export const DEFAULT_MAX_DOC_SIZE_BYTES = 1024 * 1024; // 1MB

export function chunkBucketData(
  operations: BucketDataDoc[],
  maxDocSizeBytes = DEFAULT_MAX_DOC_SIZE_BYTES
): BucketDataDoc[][] {
  const chunks: BucketDataDoc[][] = [];
  let currentChunk: BucketDataDoc[] = [];
  let currentSize = 0;

  for (const op of operations) {
    const opSize = op.data?.length ?? 0;

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
