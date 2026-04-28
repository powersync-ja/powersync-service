import { mongo } from '@powersync/lib-service-mongodb';

/**
 * Get the byte size of the current batch on a cursor.
 *
 * Call after hasNext(), before or after readBufferedDocuments().
 *
 * This is built on internal APIs, and may stop working in future driver versions.
 */
export function getCursorBatchBytes(cursor: mongo.AbstractCursor): number {
  const documents = (cursor as any).documents as CursorResponse | undefined;
  return getResponseBytes(documents);
}

// Define the internal types from the driver.
// Here we're using them defensively, assuming it may be undefined at any point.

interface CursorResponse {
  toBytes?(): Uint8Array;
}

function getResponseBytes(response: CursorResponse | undefined): number {
  const buffer = response?.toBytes?.();
  return buffer?.byteLength ?? 0;
}
