import { describe, expect, test } from 'vitest';
import { ObjectStorageLifecycle } from '../../src/storage/implementation/v3/object-storage/ObjectStorageLifecycle.js';

describe('ObjectStorageLifecycle', () => {
  test('escapes bucket names as path segments', () => {
    const lifecycle = new ObjectStorageLifecycle(null as any, 12, null as any);

    const path = lifecycle.allocatePath('definition', 'bucket/with spaces?and%escapes', 1n, 2n);

    expect(path).toMatch(/^bucket-data\/12\/definition\/bucket%2Fwith%20spaces%3Fand%25escapes\/1-2-[0-9a-f-]+\.bson$/);
  });
});
