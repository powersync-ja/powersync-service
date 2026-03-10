import { shouldRetryReplication } from '@module/replication/WalStream.js';
import { describe, expect, test } from 'vitest';

describe('shouldRetryReplication', () => {
  test('blocks retry when slot lost during snapshot', () => {
    expect(shouldRetryReplication({
      walStatus: 'lost',
      phase: 'snapshot'
    })).toBe(false);
  });

  test('allows retry when slot lost during streaming', () => {
    expect(shouldRetryReplication({
      walStatus: 'lost',
      phase: 'streaming'
    })).toBe(true);
  });

  test('allows retry when slot is missing', () => {
    expect(shouldRetryReplication({
      walStatus: 'missing',
      phase: 'snapshot'
    })).toBe(true);
  });

  test('allows retry when invalidation reason is rows_removed', () => {
    expect(shouldRetryReplication({
      walStatus: 'lost',
      phase: 'snapshot',
      invalidationReason: 'rows_removed'
    })).toBe(true);
  });
});
