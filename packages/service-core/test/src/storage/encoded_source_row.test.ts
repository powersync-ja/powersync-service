import { storage, utils } from '@/index.js';
import { describe, expect, test, vi } from 'vitest';

describe('EncodedSourceRow', () => {
  test('decodes once, on first use', () => {
    const decode = vi.fn(() => ({ id: 't1' }));
    const row = new storage.EncodedSourceRow('bson', new Uint8Array(16), decode);
    expect(decode).not.toHaveBeenCalled();

    expect(storage.decodeSourceRow(row)).toEqual({ id: 't1' });
    expect(row.toSqliteRow()).toBe(row.toSqliteRow());
    expect(decode).toHaveBeenCalledOnce();
  });

  test('passes decoded rows through', () => {
    const row = { id: 't1' };
    expect(storage.decodeSourceRow(row)).toBe(row);
    expect(storage.decodeSourceRow(undefined)).toBeUndefined();
  });

  test('does not decode for size estimates or log messages', () => {
    const decode = vi.fn(() => ({ id: 't1' }));
    const row = new storage.EncodedSourceRow('bson', new Uint8Array(16), decode);

    expect(utils.estimateRowSize(row)).toBe(16);
    expect(storage.sourceRowLogId(row, 'replica-id')).toBe('replica-id');
    expect(storage.sourceRowLogId({ id: 't1' }, 'replica-id')).toBe('t1');
    expect(decode).not.toHaveBeenCalled();
  });
});
