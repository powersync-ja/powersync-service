import { parseChangeDocument } from '@module/replication/RawChangeStream.js';
import { mongo } from '@powersync/lib-service-mongodb';
import { expect, test } from 'vitest';

test('parsed document images share the raw event allocation', () => {
  const raw = createRawEvent('first');
  const parsed = parseChangeDocument(raw);
  if (parsed.operationType != 'update') throw new Error('Expected an update');

  for (const image of [parsed.fullDocument, parsed.fullDocumentBeforeChange]) {
    expect(Buffer.isBuffer(image)).toBe(true);
    expect(image!.buffer).toBe(raw.buffer);
    expect(image!.byteOffset).toBeGreaterThan(raw.byteOffset);
    expect(image!.byteOffset + image!.byteLength).toBeLessThanOrEqual(raw.byteOffset + raw.byteLength);
  }
});

/**
 * Allocate each event independently so Node's shared small-buffer pool cannot explain retained memory.
 */
function createRawEvent(id: string): Buffer {
  const encoded = mongo.BSON.serialize({
    _id: { _data: id },
    operationType: 'update',
    ns: { db: 'app', coll: 'documents' },
    documentKey: { _id: id },
    fullDocument: { _id: id, body: 'x'.repeat(16_384) },
    fullDocumentBeforeChange: { _id: id, body: 'y'.repeat(16_384) }
  });
  const raw = Buffer.alloc(encoded.byteLength);
  raw.set(encoded);
  return raw;
}
