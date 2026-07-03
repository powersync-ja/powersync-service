import { describe, expect, test } from 'vitest';
import { SlateDBKVStore } from '../../src/storage/SlateDBKVStore.js';
import { SlateDBStorageConfig } from '../../src/types/types.js';

describe('SlateDB storage config', () => {
  test('opens an in-memory object store', async () => {
    await using store = await SlateDBKVStore.open({
      objectStore: { type: 'memory' },
      dbPath: `test-${Date.now()}`
    });

    await store.put('key', { value: 'test' });

    expect(await store.get('key')).toEqual({ value: 'test' });
  });

  test('decodes S3 object store config', () => {
    expect(
      SlateDBStorageConfig.decode({
        type: 'slatedb',
        object_store: {
          type: 's3',
          bucket: 'powersync-test',
          prefix: 'storage'
        },
        db_path: 'db'
      })
    ).toMatchObject({
      type: 'slatedb',
      object_store: {
        type: 's3',
        bucket: 'powersync-test',
        prefix: 'storage'
      },
      db_path: 'db'
    });
  });
});
