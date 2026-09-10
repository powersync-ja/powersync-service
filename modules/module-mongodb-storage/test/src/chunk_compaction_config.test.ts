import { MongoStorageConfig, normalizeChunkCompactionConcurrency } from '@module/types/types.js';
import { describe, expect, test } from 'vitest';

describe('chunk compaction concurrency configuration', () => {
  test('defaults to two workers', () => {
    expect(normalizeChunkCompactionConcurrency(undefined)).toBe(2);
  });

  test('decodes a configured worker count', () => {
    const config = MongoStorageConfig.decode({
      type: 'mongodb',
      uri: 'mongodb://localhost:27017/powersync',
      chunk_compaction_concurrency: 4
    });
    expect(normalizeChunkCompactionConcurrency(config.chunk_compaction_concurrency)).toBe(4);
  });

  test.each([1, 64])('accepts boundary worker count %s', (value) => {
    expect(normalizeChunkCompactionConcurrency(value)).toBe(value);
  });

  test.each([0, -1, 1.5, 65, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1])(
    'rejects invalid worker count %s',
    (value) => {
      expect(() => normalizeChunkCompactionConcurrency(value)).toThrow(
        'storage.chunk_compaction_concurrency must be an integer between 1 and 64'
      );
    }
  );
});
