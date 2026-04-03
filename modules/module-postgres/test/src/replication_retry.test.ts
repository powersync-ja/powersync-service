import {
  MissingReplicationSlotError,
  shouldRetryReplication
} from '@module/replication/MissingReplicationSlotError.js';
import { describe, expect, test } from 'vitest';

describe('shouldRetryReplication', () => {
  test('blocks retry when slot lost during snapshot', () => {
    const error = new MissingReplicationSlotError('test', {
      walStatus: 'lost',
      phase: 'snapshot'
    });
    expect(shouldRetryReplication(error)).toBe(false);
  });

  test('allows retry when slot lost during streaming', () => {
    const error = new MissingReplicationSlotError('test', {
      walStatus: 'lost',
      phase: 'streaming'
    });
    expect(shouldRetryReplication(error)).toBe(true);
  });

  test('allows retry when slot is missing', () => {
    const error = new MissingReplicationSlotError('test', {
      walStatus: 'missing',
      phase: 'snapshot'
    });
    expect(shouldRetryReplication(error)).toBe(true);
  });

  test('allows retry when invalidation reason is rows_removed', () => {
    const error = new MissingReplicationSlotError('test', {
      walStatus: 'lost',
      phase: 'snapshot',
      invalidationReason: 'rows_removed'
    });
    expect(shouldRetryReplication(error)).toBe(true);
  });
});
