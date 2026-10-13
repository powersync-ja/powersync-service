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

test('clearing input slots retains the raw payload allocations through parsed images', () => {
  const input: Array<Buffer | undefined> = [createRawEvent('first'), createRawEvent('second')];
  const rawBytes = input.reduce((total, event) => total + event!.byteLength, 0);
  const parsed = [];
  for (let index = 0; index < input.length; index++) {
    parsed.push(parseChangeDocument(input[index]!));
    input[index] = undefined;
  }

  expect(input).toEqual([undefined, undefined]);
  const retainedAllocations = new Set<ArrayBufferLike>();
  const ids: unknown[] = [];
  for (const event of parsed) {
    if (event.operationType != 'update') throw new Error('Expected an update');
    for (const image of [event.fullDocument, event.fullDocumentBeforeChange]) {
      retainedAllocations.add(image!.buffer);
    }
    ids.push(mongo.BSON.deserialize(event.fullDocument!)._id);
  }
  expect(ids).toEqual(['first', 'second']);
  // Removing array references cannot free these allocations: the parsed images still reference them.
  // Check backing-store identity/size instead of GC timing or noisy process-wide memory measurements.
  expect(retainedAllocations.size).toBe(2);
  expect([...retainedAllocations].reduce((total, allocation) => total + allocation.byteLength, 0)).toBe(rawBytes);
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
