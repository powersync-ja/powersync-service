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

  describe('formIntersection', () => {
    const col0 = { lookup: element(0), outputIndex: 0 };
    const col1 = { lookup: element(1), outputIndex: 0 };

    test('with a fixed value, removes rows where any column differs from it', () => {
      const rs = new ResultSet(2);
      rs.multiply(0, [['a'], ['b']]);
      rs.multiply(1, [['a'], ['b']]);

      rs.formIntersection([col0, col1], 'a');

      expect([...rs.projectUnique([col0, col1])]).toStrictEqual([['a', 'a']]);
    });

    test('without a fixed value, removes rows where the columns differ from each other', () => {
      const rs = new ResultSet(2);
      rs.multiply(0, [['a'], ['b']]);
      rs.multiply(1, [['a'], ['b']]);

      rs.formIntersection([col0, col1]);

      expect([...rs.projectUnique([col0, col1])]).toStrictEqual([
        ['a', 'a'],
        ['b', 'b']
      ]);
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
        ['c', 30],
        ['c', 31]
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
