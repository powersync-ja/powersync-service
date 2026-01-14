import { describe, expect } from 'vitest';
import { syncTest } from './utils.js';
import { TestSourceTable } from '../util.js';

describe('evaluating streams', () => {
  syncTest('without filter', ({ sync }) => {
    const desc = sync.prepareSyncStreams([{ name: 'stream', queries: ['SELECT * FROM users'] }]);

    expect(
      desc.evaluateRow({
        sourceTable: USERS,
        record: {
          id: 'foo'
        }
      })
    ).toStrictEqual([]);
  });
});

const USERS = new TestSourceTable('users');
