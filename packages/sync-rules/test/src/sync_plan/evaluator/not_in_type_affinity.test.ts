import { describe, expect } from 'vitest';
import { TestSourceTable } from '../../util.js';
import { syncTest } from './utils.js';

describe('NOT IN scalar JSON array — type affinity', () => {
  const DOCS = new TestSourceTable('docs');

  syncTest('boolean column NOT IN [true] excludes the matching row', ({ sync }) => {
    const desc = sync.prepareSyncStreams(`
config:
  edition: 3

streams:
  stream:
    auto_subscribe: true
    query: SELECT * FROM docs WHERE flag NOT IN '[true]'
`);

    expect(
      desc.evaluateRow({ sourceTable: DOCS, record: { id: 'a', flag: 1n } })
    ).toHaveLength(0);
    expect(
      desc.evaluateRow({ sourceTable: DOCS, record: { id: 'b', flag: 0n } })
    ).toHaveLength(1);
  });

  syncTest('boolean column IN [true] admits only the matching row', ({ sync }) => {
    const desc = sync.prepareSyncStreams(`
config:
  edition: 3

streams:
  stream:
    auto_subscribe: true
    query: SELECT * FROM docs WHERE flag IN '[true]'
`);

    expect(
      desc.evaluateRow({ sourceTable: DOCS, record: { id: 'a', flag: 1n } })
    ).toHaveLength(1);
    expect(
      desc.evaluateRow({ sourceTable: DOCS, record: { id: 'b', flag: 0n } })
    ).toHaveLength(0);
  });

  syncTest('large bigint column NOT IN keeps precision via JSONBig parse', ({ sync }) => {
    // 9999999999999999 exceeds Number.MAX_SAFE_INTEGER; JSON.parse would round it.
    const desc = sync.prepareSyncStreams(`
config:
  edition: 3

streams:
  stream:
    auto_subscribe: true
    query: SELECT * FROM docs WHERE big NOT IN '[9999999999999999]'
`);

    expect(
      desc.evaluateRow({ sourceTable: DOCS, record: { id: 'a', big: 9999999999999999n } })
    ).toHaveLength(0);
    expect(
      desc.evaluateRow({ sourceTable: DOCS, record: { id: 'b', big: 1n } })
    ).toHaveLength(1);
  });

  syncTest('integer column NOT IN [1] excludes 1n and admits 2n', ({ sync }) => {
    const desc = sync.prepareSyncStreams(`
config:
  edition: 3

streams:
  stream:
    auto_subscribe: true
    query: SELECT * FROM docs WHERE n NOT IN '[1]'
`);

    expect(desc.evaluateRow({ sourceTable: DOCS, record: { id: 'a', n: 1n } })).toHaveLength(0);
    expect(desc.evaluateRow({ sourceTable: DOCS, record: { id: 'b', n: 2n } })).toHaveLength(1);
  });

  syncTest('string variant still works (regression guard)', ({ sync }) => {
    const desc = sync.prepareSyncStreams(`
config:
  edition: 3

streams:
  stream:
    auto_subscribe: true
    query: SELECT * FROM docs WHERE state NOT IN '["foo"]'
`);

    expect(
      desc.evaluateRow({ sourceTable: DOCS, record: { id: 'a', state: 'foo' } })
    ).toHaveLength(0);
    expect(
      desc.evaluateRow({ sourceTable: DOCS, record: { id: 'b', state: 'bar' } })
    ).toHaveLength(1);
  });
});
