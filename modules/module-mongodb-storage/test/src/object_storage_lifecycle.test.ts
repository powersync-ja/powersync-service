import { createHash } from 'node:crypto';
import { describe, expect, test } from 'vitest';
import { ObjectStorageLifecycle } from '../../src/storage/implementation/v3/object-storage/ObjectStorageLifecycle.js';
import { MemoryObjectStorage } from './helpers/MemoryObjectStorage.js';

describe('ObjectStorageLifecycle', () => {
  test('escapes bucket names as path segments', () => {
    const lifecycle = new ObjectStorageLifecycle(null as any, 12, null as any);

    const path = lifecycle.allocatePath('definition', 'bucket/with spaces?and%escapes', 1n, 2n);

    expect(path).toMatch(/^bucket-data\/12\/definition\/bucket%2Fwith%20spaces%3Fand%25escapes\/1-2-[0-9a-f-]+\.bson$/);
  });

  test('hashes bucket path segments longer than 128 bytes', () => {
    const lifecycle = new ObjectStorageLifecycle(null as any, 12, null as any);
    const bucket = '💾/long bucket parameter/'.repeat(20);
    const hash = createHash('sha256').update(bucket).digest('base64url');

    const path = lifecycle.allocatePath('definition', bucket, 1n, 2n);

    expect(path).toMatch(new RegExp(`^bucket-data/12/definition/sha256-${hash}/1-2-[0-9a-f-]+\\.bson$`));
  });

  test('deletePrefix removes only objects under the exact prefix', async () => {
    const objectStorage = new MemoryObjectStorage();
    const lifecycle = new ObjectStorageLifecycle(null as any, 1, objectStorage);
    for (const path of [
      'bucket-data/1/definition/object-1.bson',
      'bucket-data/12/definition/object-2.bson',
      'bucket-data/1-other/definition/object-3.bson'
    ]) {
      await objectStorage.put(path, new Uint8Array(), {
        contentType: 'application/bson',
        contentEncoding: null
      });
    }

    await expect(lifecycle.deletePrefix(lifecycle.streamPrefix())).resolves.toEqual({ objectCount: 1 });
    expect([...objectStorage.store.keys()]).toEqual([
      'bucket-data/12/definition/object-2.bson',
      'bucket-data/1-other/definition/object-3.bson'
    ]);
  });

  test('definition prefixes do not match ids with the same string prefix', async () => {
    const objectStorage = new MemoryObjectStorage();
    const lifecycle = new ObjectStorageLifecycle(null as any, 7, objectStorage);
    for (const path of ['bucket-data/7/1/object-1.bson', 'bucket-data/7/12/object-2.bson']) {
      await objectStorage.put(path, new Uint8Array(), {
        contentType: 'application/bson',
        contentEncoding: null
      });
    }

    await lifecycle.deletePrefix(lifecycle.definitionPrefix('1'));
    expect([...objectStorage.store.keys()]).toEqual(['bucket-data/7/12/object-2.bson']);
  });
});
