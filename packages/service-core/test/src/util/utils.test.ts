import { formatBytes } from '@/index.js';
import { describe, expect, test } from 'vitest';

describe('formatBytes', () => {
  test('formats bytes', () => {
    expect(formatBytes(500)).toBe('500B');
  });

  test('formats kilobytes', () => {
    expect(formatBytes(1024)).toBe('1.0KB');
    expect(formatBytes(1536)).toBe('1.5KB');
  });

  test('formats megabytes', () => {
    expect(formatBytes(1024 * 1024)).toBe('1.0MB');
    expect(formatBytes(1.5 * 1024 * 1024)).toBe('1.5MB');
  });

  test('formats gigabytes', () => {
    expect(formatBytes(1024 * 1024 * 1024)).toBe('1.0GB');
    expect(formatBytes(10.5 * 1024 * 1024 * 1024)).toBe('10.5GB');
  });
});
