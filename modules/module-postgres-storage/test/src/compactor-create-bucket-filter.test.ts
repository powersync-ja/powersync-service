import { describe, expect, test } from 'vitest';
import { createBucketFilter, isValidBucketNameOrPrefix } from '../../src/storage/PostgresCompactor.js';

describe('createBucketFilter', () => {
  test('undefined input returns all mode', () => {
    const filter = createBucketFilter(undefined);
    expect(filter).toEqual({ mode: 'all' });
  });

  test('global bucket with empty brackets returns exact mode', () => {
    const filter = createBucketFilter('global[]');
    expect(filter).toEqual({ mode: 'exact', bucketName: 'global[]' });
  });

  test('parameterized bucket returns exact mode', () => {
    const filter = createBucketFilter('user["user-123"]');
    expect(filter).toEqual({ mode: 'exact', bucketName: 'user["user-123"]' });
  });

  test('bucket with multiple parameters returns exact mode', () => {
    const filter = createBucketFilter('workspace["ws-1","org-2"]');
    expect(filter).toEqual({ mode: 'exact', bucketName: 'workspace["ws-1","org-2"]' });
  });

  test('bucket with numeric parameter returns exact mode', () => {
    const filter = createBucketFilter('item["12345"]');
    expect(filter).toEqual({ mode: 'exact', bucketName: 'item["12345"]' });
  });

  test('bucket with UUID parameter returns exact mode', () => {
    const filter = createBucketFilter('session["550e8400-e29b-41d4-a716-446655440000"]');
    expect(filter).toEqual({ mode: 'exact', bucketName: 'session["550e8400-e29b-41d4-a716-446655440000"]' });
  });

  test('simple name returns prefix mode', () => {
    const filter = createBucketFilter('user');
    expect(filter).toEqual({ mode: 'prefix', bucketPrefix: 'user[' });
  });

  test('name with underscores returns prefix mode', () => {
    const filter = createBucketFilter('by_user_org');
    expect(filter).toEqual({ mode: 'prefix', bucketPrefix: 'by_user_org[' });
  });

  test('name with hyphens returns prefix mode', () => {
    const filter = createBucketFilter('my-bucket');
    expect(filter).toEqual({ mode: 'prefix', bucketPrefix: 'my-bucket[' });
  });

  test('single character name returns prefix mode', () => {
    const filter = createBucketFilter('a');
    expect(filter).toEqual({ mode: 'prefix', bucketPrefix: 'a[' });
  });

  test('name with numbers returns prefix mode', () => {
    const filter = createBucketFilter('bucket123');
    expect(filter).toEqual({ mode: 'prefix', bucketPrefix: 'bucket123[' });
  });

  test('bucket with escaped quotes in parameter', () => {
    const filter = createBucketFilter('user["name\\"with\\"quotes"]');
    expect(filter).toEqual({ mode: 'exact', bucketName: 'user["name\\"with\\"quotes"]' });
  });

  test('bucket with special SQL characters', () => {
    const filter = createBucketFilter("user[\"'; DROP TABLE users; --\"]");
    expect(filter).toEqual({ mode: 'exact', bucketName: "user[\"'; DROP TABLE users; --\"]" });
  });

  test('bucket with unicode characters', () => {
    const filter = createBucketFilter('用户["测试"]');
    expect(filter).toEqual({ mode: 'exact', bucketName: '用户["测试"]' });
  });

  test('very long bucket definition name', () => {
    const longName = 'a'.repeat(1000);
    const filter = createBucketFilter(longName);
    expect(filter).toEqual({ mode: 'prefix', bucketPrefix: `${longName}[` });
  });

  test('bucket with nested brackets', () => {
    const filter = createBucketFilter('data["array[0]"]');
    expect(filter).toEqual({ mode: 'exact', bucketName: 'data["array[0]"]' });
  });

  test('name with whitespace returns prefix mode', () => {
    const filter = createBucketFilter('my bucket');
    expect(filter).toEqual({ mode: 'prefix', bucketPrefix: 'my bucket[' });
  });

  test('bucket with newline in parameter', () => {
    const filter = createBucketFilter('user["line1\\nline2"]');
    expect(filter).toEqual({ mode: 'exact', bucketName: 'user["line1\\nline2"]' });
  });

  test('throws on empty string', () => {
    expect(() => createBucketFilter('')).toThrow(/Invalid bucket filter/);
  });

  test('throws on incomplete bracket', () => {
    expect(() => createBucketFilter('user[')).toThrow(/Invalid bucket filter/);
  });

  test('throws on incomplete parameter', () => {
    expect(() => createBucketFilter('user["')).toThrow(/Invalid bucket filter/);
  });

  test('throws on extra characters after closing bracket', () => {
    expect(() => createBucketFilter('user[]extra')).toThrow(/Invalid bucket filter/);
  });

  test('throws on malformed JSON in parameters', () => {
    expect(() => createBucketFilter('user[invalid]')).toThrow(/Invalid bucket filter/);
  });
});

describe('isValidBucketNameOrPrefix', () => {
  test('valid bucket definition name', () => {
    expect(isValidBucketNameOrPrefix('user')).toBe(true);
  });

  test('valid bucket name with empty params', () => {
    expect(isValidBucketNameOrPrefix('global[]')).toBe(true);
  });

  test('valid bucket name with single param', () => {
    expect(isValidBucketNameOrPrefix('user["user-123"]')).toBe(true);
  });

  test('valid bucket name with multiple params', () => {
    expect(isValidBucketNameOrPrefix('workspace["ws-1","org-2"]')).toBe(true);
  });

  test('valid unicode bucket name', () => {
    expect(isValidBucketNameOrPrefix('用户["测试"]')).toBe(true);
  });

  test('valid bucket with escaped quotes', () => {
    expect(isValidBucketNameOrPrefix('user["name\\"with\\"quotes"]')).toBe(true);
  });

  test('empty string is invalid', () => {
    expect(isValidBucketNameOrPrefix('')).toBe(false);
  });

  test('incomplete bracket is invalid', () => {
    expect(isValidBucketNameOrPrefix('user[')).toBe(false);
  });

  test('incomplete parameter is invalid', () => {
    expect(isValidBucketNameOrPrefix('user["')).toBe(false);
  });

  test('extra characters after bracket is invalid', () => {
    expect(isValidBucketNameOrPrefix('user[]extra')).toBe(false);
  });

  test('malformed JSON is invalid', () => {
    expect(isValidBucketNameOrPrefix('user[invalid]')).toBe(false);
  });
});
