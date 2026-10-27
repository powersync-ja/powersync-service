import { describe, expect, test } from 'vitest';
import { ResultSet, ResultSetElement } from '../../../../src/sync_plan/evaluator/result_set.js';
import { SqliteParameterValue } from '../../../../src/types.js';

describe('ResultSet', () => {
  test('unit result set', () => {
    const empty = new ResultSet(0);

    expect(empty.length).toStrictEqual(1);
    expect([...empty.projectUnique([])]).toStrictEqual([[]]);
  });

  describe('projectUnique', () => {
    const rs = new ResultSet(1);
    rs.multiply(0, [
      ['a1', 'b1'],
      ['a2', 'b2'],
      ['a2', 'b1'],
      ['a2', 'b2']
    ]);

    test('single column', () => {
      expect([...rs.projectUnique([{ lookup: element(0), outputIndex: 0 }])]).toStrictEqual([['a1'], ['a2']]);

      expect([...rs.projectUnique([{ lookup: element(0), outputIndex: 1 }])]).toStrictEqual([['b1'], ['b2']]);
    });

    test('multiple columns', () => {
      expect([
        ...rs.projectUnique([
          { lookup: element(0), outputIndex: 0 },
          { lookup: element(0), outputIndex: 1 }
        ])
      ]).toStrictEqual([
        ['a1', 'b1'],
        ['a2', 'b2'],
        ['a2', 'b1']
      ]);
    });
  });

  describe('multiply', () => {
    test('empty', () => {
      const rs = new ResultSet(1);
      expect(rs.length).toStrictEqual(1);

      rs.multiply(0, []);
      expect(rs.length).toStrictEqual(0);
    });

    test('is cartesian product', () => {
      const rs = new ResultSet(2);

      rs.multiply(0, [['a'], ['b']]);
      rs.multiply(1, [[0], [1]]);
      expect([
        ...rs.projectUnique([
          {
            lookup: element(0),
            outputIndex: 0
          },
          {
            lookup: element(1),
            outputIndex: 0
          }
        ])
      ]).toStrictEqual([
        ['a', 0],
        ['b', 0],
        ['a', 1],
        ['b', 1]
      ]);
    });
  });

  describe('multiply with join keys', () => {
    const col0 = { lookup: element(0), outputIndex: 0 };
    const col1 = { lookup: element(1), outputIndex: 0 };
    const col1b = { lookup: element(1), outputIndex: 1 };

    test('only combines rows with matching keys', () => {
      const rs = new ResultSet(2);
      rs.multiply(0, [['a'], ['b'], ['c']]);
      rs.multiply(
        1,
        [
          ['a', 1],
          ['a', 2],
          ['b', 3],
          ['d', 4]
        ],
        { keys: [{ existing: col0, addedIndex: 0 }], semiJoin: false }
      );

      expect(rs.length).toStrictEqual(3);
      expect(new Set(rs.projectUnique([col0, col1b]))).toStrictEqual(
        new Set([
          ['a', 1],
          ['a', 2],
          ['b', 3]
        ])
      );
    });

    test('composite keys', () => {
      const rs = new ResultSet(2);
      rs.multiply(0, [
        [1, 'x'],
        [1, 'y'],
        [2, 'x']
      ]);
      rs.multiply(
        1,
        [
          [1, 'y'],
          [2, 'y'],
          [2, 'x']
        ],
        {
          keys: [
            { existing: col0, addedIndex: 0 },
            { existing: { lookup: element(0), outputIndex: 1 }, addedIndex: 1 }
          ],
          semiJoin: false
        }
      );

      expect(new Set(rs.projectUnique([col1, col1b]))).toStrictEqual(
        new Set([
          [1, 'y'],
          [2, 'x']
        ])
      );
    });

    test('semi join only filters rows', () => {
      const rs = new ResultSet(2);
      rs.multiply(0, [['a'], ['b'], ['c']]);
      // Duplicate matches must not multiply existing rows.
      rs.multiply(1, [['a'], ['a'], ['c']], { keys: [{ existing: col0, addedIndex: 0 }], semiJoin: true });

      expect(rs.length).toStrictEqual(2);
      expect(new Set(rs.projectUnique([col0]))).toStrictEqual(new Set([['a'], ['c']]));
    });

    test('semi join without keys keeps all rows', () => {
      const rs = new ResultSet(2);
      rs.multiply(0, [['a'], ['b']]);
      rs.multiply(1, [['x'], ['y']], { keys: [], semiJoin: true });

      expect(rs.length).toStrictEqual(2);
    });
  });

  describe('joinAsync', () => {
    const col0 = { lookup: element(0), outputIndex: 0 };
    const col1 = { lookup: element(1), outputIndex: 0 };

    test('expands each row with matching values, deduplicating lookups', async () => {
      const rs = new ResultSet(2);
      rs.multiply(0, [['a'], ['b'], ['a']]);

      const matches: Record<string, SqliteParameterValue[][]> = {
        a: [[1], [2]],
        b: [[3]]
      };

      let lookupCount = 0;
      await rs.joinAsync([col0], 1, async (lookups) => {
        // The lookup for 'a' is only performed once, even though it's shared by two rows.
        expect(lookups.length).toStrictEqual(2);
        lookupCount++;

        for (const lookup of lookups) {
          lookup.foundRows.push(...matches[lookup.inputs[0] as string]);
        }
      });

      expect(lookupCount).toStrictEqual(1);
      expect([...rs.projectUnique([col0, col1])]).toStrictEqual([
        ['a', 1],
        ['b', 3],
        ['a', 2]
      ]);
    });

    test('removes rows without a matching join partner', async () => {
      const rs = new ResultSet(2);
      rs.multiply(0, [['a'], ['b'], ['c'], ['a']]);

      const matches: Record<string, SqliteParameterValue[][]> = {
        a: [[10]],
        b: [],
        c: [[30], [31]]
      };

      await rs.joinAsync([col0], 1, async (lookups) => {
        for (const lookup of lookups) {
          lookup.foundRows.push(...matches[lookup.inputs[0] as string]);
        }
      });

      expect(rs.length).toStrictEqual(4);
      expect([...rs.projectUnique([col0, col1])]).toStrictEqual([
        ['a', 10],
        ['c', 31],
        ['c', 30]
      ]);
    });

    test('removing all rows results in an empty result set', async () => {
      const rs = new ResultSet(2);
      rs.multiply(0, [['a'], ['b']]);

      await rs.joinAsync([col0], 1, async () => {
        // Leave foundRows empty for every lookup.
      });

      expect(rs.length).toStrictEqual(0);
      expect([...rs.projectUnique([col0, col1])]).toStrictEqual([]);
    });

    test('supports composite join keys', async () => {
      const rs = new ResultSet(2);
      rs.multiply(0, [
        [1, 'x'],
        [1, 'y'],
        [2, 'x']
      ]);

      const colKey0 = { lookup: element(0), outputIndex: 0 };
      const colKey1 = { lookup: element(0), outputIndex: 1 };

      const matches: Record<string, SqliteParameterValue[][]> = {
        '1,x': [[100]],
        '1,y': [[200]],
        '2,x': [[300], [301]]
      };

      await rs.joinAsync([colKey0, colKey1], 1, async (lookups) => {
        for (const lookup of lookups) {
          lookup.foundRows.push(...matches[lookup.inputs.join(',')]);
        }
      });

      expect([...rs.projectUnique([colKey0, colKey1, col1])]).toStrictEqual([
        [1, 'x', 100],
        [1, 'y', 200],
        [2, 'x', 300],
        [2, 'x', 301]
      ]);
    });

    test('with join keys', async () => {
      const rs = new ResultSet(2);
      rs.multiply(0, [
        ['a', 'k1'],
        ['b', 'k1'],
        ['c', 'k2']
      ]);

      // The lookup is keyed on column 1, but found rows must also match column 0.
      await rs.joinAsync(
        [{ lookup: element(0), outputIndex: 1 }],
        1,
        async (lookups) => {
          expect(lookups).toHaveLength(2);
          for (const lookup of lookups) {
            if (lookup.inputs[0] == 'k1') {
              lookup.foundRows.push(['a', 1], ['a', 2], ['c', 3]);
            } else {
              lookup.foundRows.push(['a', 4], ['c', 5]);
            }
          }
        },
        { keys: [{ existing: col0, addedIndex: 0 }], semiJoin: false }
      );

      expect(new Set(rs.projectUnique([col0, { lookup: element(1), outputIndex: 1 }]))).toStrictEqual(
        new Set([
          ['a', 1],
          ['a', 2],
          ['c', 5]
        ])
      );
    });

    test('empty join key', async () => {
      const rs = new ResultSet(2);
      rs.multiply(0, [['a'], ['b']]);

      await rs.joinAsync([], 1, async (lookups) => {
        expect(lookups).toMatchObject([{ inputs: [] }]);

        lookups[0].foundRows.push(['x']);
      });

      expect([...rs.projectUnique([col0, col1])]).toStrictEqual([
        ['a', 'x'],
        ['b', 'x']
      ]);
    });
  });
});

function element(index: number): ResultSetElement {
  return { resultSetIndex: index };
}
