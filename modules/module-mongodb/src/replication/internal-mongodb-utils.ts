import { mongo } from '@powersync/lib-service-mongodb';
import { get } from 'http';

/**
 * Track bytes read on a change stream.
 *
 * This is after decompression, and without TLS overhead.
 *
 * This excludes some protocol overhead, but does include per-batch overhead.
 *
 * This is built on internal APIs, and may stop working in future driver versions.
 *
 * @param add Called once for each batch of data.
 */
export function trackChangeStreamBsonBytes(changeStream: mongo.ChangeStream, add: (bytes: number) => void) {
  let internalChangeStream = changeStream as ChangeStreamWithCursor;
  let current = internalChangeStream.cursor;
  let degisterCursor = trackCursor(current, add);

  const refresh = () => {
    // The cursor may be replaced closed and re-opened (replaced) in various scenarios, such as
    // after a primary fail-over event.
    // There is no direct even to track that, but the `resumeTokenChanged` event is a good proxy.
    // It may be called more often than the cursor is replaced, so we just check whether the cursor changed.
    // This might miss the init batch, so we may under-count slightly in that case. It is a rare event
    // and typically a small number of bytes, so it's fine to ignore.
    const next = internalChangeStream.cursor;
    if (next !== current) {
      degisterCursor();
      current = next;
      degisterCursor = trackCursor(current, add);
    }
  };

  changeStream.on('resumeTokenChanged', refresh);

  // We return this to allow de-registration of the event listeners.
  // However, these are garbage collected automatically when the stream is closed, so it's not strictly necessary to call this.
  return () => {
    changeStream.off('resumeTokenChanged', refresh);
  };
}

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

interface ChangeStreamWithCursor extends mongo.ChangeStream {
  cursor?: mongo.AbstractCursor;
}

function trackCursor(cursor: mongo.AbstractCursor | undefined, add: (bytes: number) => void) {
  if (cursor == null) {
    return () => {};
  }
  const countBatch = (response: CursorResponse | undefined) => {
    const bytes = getResponseBytes(response);
    if (bytes > 0) {
      add(bytes);
    }
  };

  // The `init` event is emitted for the first batch, and the `more` event is emitted for subsequent batches.
  cursor.on('init', countBatch);
  cursor.on('more', countBatch);

  return () => {
    cursor.off('init', countBatch);
    cursor.off('more', countBatch);
  };
}

function getResponseBytes(response: CursorResponse | undefined): number {
  const buffer = response?.toBytes?.();
  return buffer?.byteLength ?? 0;
}
