import { describe, expect } from 'vitest';
import { TestSourceTable } from '../../util.js';
import { syncTest } from './utils.js';

describe('SQLite NULL semantics for NOT in sync stream filters', () => {
  const DOCS = new TestSourceTable('docs');

  syncTest('NOT over a NULL scalar expression should not admit the row', ({ sync }) => {
    const desc = sync.prepareSyncStreams(`
config:
  edition: 3

streams:
  stream:
    auto_subscribe: true
    query: SELECT * FROM docs WHERE NOT nullable_flag
`);

    expect(desc.evaluateRow({ sourceTable: DOCS, record: { id: 'null-row', nullable_flag: null } })).toHaveLength(0);
    expect(desc.evaluateRow({ sourceTable: DOCS, record: { id: 'false-row', nullable_flag: 0n } })).toHaveLength(1);
    expect(desc.evaluateRow({ sourceTable: DOCS, record: { id: 'true-row', nullable_flag: 1n } })).toHaveLength(0);
  });

  syncTest('NOT IN over a NULL left operand should not admit the row', ({ sync }) => {
    const desc = sync.prepareSyncStreams(`
config:
  edition: 3

streams:
  stream:
    auto_subscribe: true
    query: SELECT * FROM docs WHERE state NOT IN '["deleted", "archived"]'
`);

    expect(desc.evaluateRow({ sourceTable: DOCS, record: { id: 'null-row', state: null } })).toHaveLength(0);
    expect(desc.evaluateRow({ sourceTable: DOCS, record: { id: 'public-row', state: 'public' } })).toHaveLength(1);
    expect(desc.evaluateRow({ sourceTable: DOCS, record: { id: 'deleted-row', state: 'deleted' } })).toHaveLength(0);
  });
});
