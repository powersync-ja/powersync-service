import { describe, expect, test } from 'vitest';
import type {
  ReplicationBenchmarkMutation,
  ReplicationBenchmarkTransaction
} from '../../../types/ReplicationBenchmark.js';
import { mongoSourceRequiresOrderedWrites } from './MongoReplicationSourceAdapter.js';

function transaction(
  entries: readonly (readonly [ReplicationBenchmarkMutation['tag'], string])[]
): ReplicationBenchmarkTransaction {
  return {
    id: 'transaction',
    position: '1',
    mutations: entries.map(([tag, id]) => ({
      tag,
      row: { id, owner_id: 'owner', category: 'test', version: 1, updated_at: '', payload: '', is_target: 0 }
    }))
  };
}

describe('MongoDB source transaction batching', () => {
  test('allows independent mixed operations to be grouped by type', () => {
    expect(
      mongoSourceRequiresOrderedWrites(
        transaction([
          ['insert', 'a'],
          ['update', 'b'],
          ['delete', 'c'],
          ['insert', 'd']
        ])
      )
    ).toBe(false);
  });

  test.each([
    ['insert', 'update'],
    ['delete', 'insert'],
    ['update', 'delete'],
    ['update', 'update']
  ] as const)('preserves dependent %s / %s operations', (first, second) => {
    expect(
      mongoSourceRequiresOrderedWrites(
        transaction([
          [first, 'a'],
          ['insert', 'other'],
          [second, 'a']
        ])
      )
    ).toBe(true);
  });

  test('checks the entire transaction, not just adjacent operations', () => {
    const entries: [ReplicationBenchmarkMutation['tag'], string][] = Array.from({ length: 20_000 }, (_, index) => [
      'insert',
      `row-${index}`
    ]);
    expect(mongoSourceRequiresOrderedWrites(transaction(entries))).toBe(false);
    entries.push(['update', 'row-0']);
    expect(mongoSourceRequiresOrderedWrites(transaction(entries))).toBe(true);
  });
});
