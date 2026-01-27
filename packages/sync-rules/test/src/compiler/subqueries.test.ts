import { describe, expect, test } from 'vitest';
import { compilationErrorsForSingleStream, compileToSyncPlanWithoutErrors } from './utils.js';

describe('subqueries', () => {
  describe('names', () => {
    test('from inner columns', () => {
      const plan = compileToSyncPlanWithoutErrors([
        { name: 'a', queries: ['SELECT * FROM (SELECT id, name FROM users) AS subquery'] }
      ]);
      const sources = plan.dataSources;
      expect(sources).toHaveLength(1);

      const [source] = sources;
      expect(source.sourceTable.tablePattern).toBe('users');

      expect(source.columns.map((e) => (e as any).alias)).toEqual(['id', 'name']);
    });

    test('from outer alias', () => {
      const plan = compileToSyncPlanWithoutErrors([
        { name: 'a', queries: ['SELECT * FROM (SELECT id, name FROM users) AS subquery (my_id, custom_name)'] }
      ]);
      const sources = plan.dataSources;
      expect(sources).toHaveLength(1);

      const [source] = sources;
      expect(source.sourceTable.tablePattern).toBe('users');

      expect(source.columns.map((e) => (e as any).alias)).toEqual(['my_id', 'custom_name']);
    });
  });

  describe('errors', () => {
    test('select star', () => {
      expect(compilationErrorsForSingleStream('SELECT 1 FROM (SELECT * FROM users) AS u')).toStrictEqual([
        {
          message: '* columns are not allowed in subqueries or common table expressions',
          source: '*'
        },
        {
          message: 'Must have a result column selecting from a table',
          source: 'SELECT 1 FROM (SELECT * FROM users) AS u'
        }
      ]);
    });

    test('name vs colum mismatch', () => {
      expect(compilationErrorsForSingleStream('SELECT a FROM (SELECT id FROM users) AS u (a, b, c)')).toStrictEqual([
        {
          message: 'Expected this subquery to have 3 columns, it actually has 1',
          source: '(SELECT id FROM users)'
        }
      ]);
    });

    test('duplicate column', () => {
      expect(compilationErrorsForSingleStream('SELECT id FROM (SELECT id, a AS id FROM users) AS u')).toStrictEqual([
        {
          message: "There is a column named 'id' already.",
          source: 'a AS id'
        }
      ]);
    });
  });
});
