import { describe, expect, test } from 'vitest';
import { compilationErrorsForSingleStream } from './utils.js';

describe('compilation errors', () => {
  test('not a select statement', () => {
    expect(compilationErrorsForSingleStream("INSERT INTO users (id) VALUES ('foo')")).toStrictEqual([
      {
        message: 'Expected a SELECT statement',
        source: "INSERT INTO users (id) VALUES ('foo'"
      }
    ]);
  });

  test('IN operator with static left clause', () => {
    expect(
      compilationErrorsForSingleStream("SELECT * FROM issues WHERE 'static' IN (SELECT id FROM users WHERE is_admin)")
    ).toStrictEqual([
      {
        message: 'This filter is unrelated to the request or the table being synced, and not supported.',
        source: "'static' IN (SELECT id FROM users WHERE is_admin"
      }
    ]);
  });

  test('negated subquery', () => {
    expect(
      compilationErrorsForSingleStream(
        'select * from comments where issue_id not in (select id from issues where owner_id = auth.user_id())'
      )
    ).toStrictEqual([
      {
        message:
          "This expression already references 'comments', so it can't also reference data from this row unless the two are compared with an equals operator.",
        source: 'id'
      },
      {
        message:
          "This expression already references row data, so it can't also reference connection parameters unless the two are compared with an equals operator.",
        source: 'auth.user_id()'
      }
    ]);
  });

  test('negated subquery from outer not operator', () => {
    expect(
      compilationErrorsForSingleStream(
        'select * from comments where not (issue_id in (select id from issues where owner_id = auth.user_id()))'
      )
    ).toStrictEqual([
      {
        message:
          "This expression already references 'comments', so it can't also reference data from this row unless the two are compared with an equals operator.",
        source: 'id'
      },
      {
        message:
          "This expression already references row data, so it can't also reference connection parameters unless the two are compared with an equals operator.",
        source: 'auth.user_id()'
      }
    ]);
  });

  test('subquery with two columns', () => {
    expect(
      compilationErrorsForSingleStream(
        'select * from comments where issue_id in (select id, owner_id from issues where owner_id = auth.user_id())'
      )
    ).toStrictEqual([
      {
        message: 'Must return a single expression column',
        source: 'select id, owner_id from issues where owner_id = auth.user_id()'
      }
    ]);
  });

  test('legacy request function', () => {
    expect(compilationErrorsForSingleStream('select * from issues where owner_id = request.user_id()')).toStrictEqual([
      { message: 'Invalid schema in function name', source: 'request.user_id' }
    ]);
  });
});
