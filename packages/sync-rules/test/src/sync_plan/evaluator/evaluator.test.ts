import { describe, expect } from 'vitest';
import { syncTest } from './utils.js';
import {
  HydratedSyncRules,
  ScopedParameterLookup,
  SourceTableInterface,
  SqliteRow,
  SqliteValue
} from '../../../../src/index.js';
import { TestSourceTable } from '../../util.js';

describe('evaluating rows', () => {
  syncTest('emits rows', ({ sync }) => {
    const desc = sync.prepareSyncStreams([{ name: 'stream', queries: ['SELECT * FROM users'] }]);

    expect(
      desc.evaluateRow({
        sourceTable: USERS,
        record: {
          id: 'foo',
          _double: 1,
          _int: 1n,
          _null: null,
          _text: 'text',
          _blob: new Uint8Array(10) // non-JSON columns should be removed
        }
      })
    ).toStrictEqual([
      {
        bucket: 'stream|0[]',
        id: 'foo',
        data: { id: 'foo', _double: 1, _int: 1n, _null: null, _text: 'text' },
        table: 'users'
      }
    ]);
  });

  syncTest('forwards parameters', ({ sync }) => {
    const desc = sync.prepareSyncStreams([
      { name: 'stream', queries: ["SELECT * FROM users WHERE value = subscription.parameter('p')"] }
    ]);

    function evaluate(value: SqliteValue) {
      const rows = desc.evaluateRow({ sourceTable: USERS, record: { id: 'foo', value } });
      if (rows.length == 0) {
        return undefined;
      }

      return rows[0].bucket;
    }

    expect(evaluate(1)).toStrictEqual('stream|0[1]');
    expect(evaluate(1n)).toStrictEqual('stream|0[1]');
    expect(evaluate(1.1)).toStrictEqual('stream|0[1.1]');
    expect(evaluate('1')).toStrictEqual('stream|0["1"]');

    // null is not equal to itself, so WHERE null = subscription.paraeter('p') should not match any rows.
    expect(evaluate(null)).toStrictEqual(undefined);

    // We can't store binary values in bucket parameters
    expect(evaluate(new Uint8Array(10))).toStrictEqual(undefined);
  });

  syncTest('output table name', ({ sync }) => {
    const desc = sync.prepareSyncStreams([{ name: 'stream', queries: ['SELECT * FROM users u'] }]);
    expect(
      desc.evaluateRow({
        sourceTable: USERS,
        record: {
          id: 'foo'
        }
      })
    ).toStrictEqual([
      {
        bucket: 'stream|0[]',
        id: 'foo',
        data: { id: 'foo' },
        table: 'u'
      }
    ]);
  });

  syncTest('wildcard with alias', ({ sync }) => {
    const desc = sync.prepareSyncStreams([{ name: 'stream', queries: ['SELECT * FROM "%" output'] }]);
    expect(
      desc.evaluateRow({
        sourceTable: USERS,
        record: {
          id: 'foo'
        }
      })
    ).toStrictEqual([
      {
        bucket: 'stream|0[]',
        id: 'foo',
        data: { id: 'foo' },
        table: 'output'
      }
    ]);
  });

  syncTest('wildcard without alias', ({ sync }) => {
    const desc = sync.prepareSyncStreams([{ name: 'stream', queries: ['SELECT * FROM "%"'] }]);
    expect(
      desc.evaluateRow({
        sourceTable: USERS,
        record: {
          id: 'foo'
        }
      })
    ).toStrictEqual([
      {
        bucket: 'stream|0[]',
        id: 'foo',
        data: { id: 'foo' },
        table: 'users'
      }
    ]);
  });

  syncTest('multiple tables in bucket', ({ sync }) => {
    const desc = sync.prepareSyncStreams([
      { name: 'stream', queries: ['SELECT * FROM users', 'SELECT * FROM comments'] }
    ]);
    expect(evaluateBucketIds(desc, USERS, { id: 'foo' })).toStrictEqual(['stream|0[]']);
    expect(evaluateBucketIds(desc, COMMENTS, { id: 'foo2' })).toStrictEqual(['stream|0[]']);
  });
});

describe('evaluating parameters', () => {
  syncTest('emits parameters', ({ sync }) => {
    const desc = sync.prepareSyncStreams([
      {
        name: 'stream',
        queries: ['SELECT * FROM comments WHERE issue_id IN (SELECT id FROM issues WHERE owner_id = auth.user_id())']
      }
    ]);

    expect(desc.tableSyncsData(COMMENTS)).toBeTruthy();
    expect(desc.tableSyncsData(ISSUES)).toBeFalsy();
    expect(desc.tableSyncsParameters(ISSUES)).toBeTruthy();

    expect(desc.evaluateParameterRow(ISSUES, { id: 'issue_id', owner_id: 'user1', name: 'name' })).toStrictEqual([
      {
        lookup: ScopedParameterLookup.direct({ lookupName: 'lookup', queryId: '0' }, ['user1']),
        bucketParameters: [
          {
            '0': 'issue_id'
          }
        ]
      }
    ]);
  });

  syncTest('skips null and binary values', ({ sync }) => {
    const desc = sync.prepareSyncStreams([
      {
        name: 'stream',
        queries: ['SELECT * FROM comments WHERE issue_id IN (SELECT id FROM issues WHERE owner_id = auth.user_id())']
      }
    ]);
    const blob = new Uint8Array(10);

    expect(desc.evaluateParameterRow(ISSUES, { id: 'issue_id', owner_id: 'user1' })).toHaveLength(1);
    expect(desc.evaluateParameterRow(ISSUES, { id: 'issue_id', owner_id: null })).toHaveLength(0);
    expect(desc.evaluateParameterRow(ISSUES, { id: null, owner_id: 'user1' })).toHaveLength(0);

    expect(desc.evaluateParameterRow(ISSUES, { id: 'issue_id', owner_id: blob })).toHaveLength(0);
    expect(desc.evaluateParameterRow(ISSUES, { id: blob, owner_id: 'user1' })).toHaveLength(0);
  });
});

function evaluateBucketIds(source: HydratedSyncRules, sourceTable: SourceTableInterface, record: SqliteRow) {
  return source.evaluateRow({ sourceTable, record }).map((r) => r.bucket);
}

const USERS = new TestSourceTable('users');
const COMMENTS = new TestSourceTable('comments');
const ISSUES = new TestSourceTable('issues');
