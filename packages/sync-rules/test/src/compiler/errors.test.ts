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

  test('not selecting from anything', () => {
    expect(compilationErrorsForSingleStream('SELECT 1, 2, 3')).toStrictEqual([
      {
        message: 'Must have a result column selecting from a table',
        source: 'SELECT 1, 2, 3'
      }
    ]);
  });

  test('selecting table-valued function', () => {
    expect(compilationErrorsForSingleStream("SELECT * FROM json_each(auth.parameter('x'))")).toStrictEqual([
      {
        message: 'Sync streams can only select from actual tables',
        source: '*'
      },
      {
        message: 'Must have a result column selecting from a table',
        source: "SELECT * FROM json_each(auth.parameter('x'))"
      }
    ]);
  });

  test('join with using', () => {
    expect(compilationErrorsForSingleStream('SELECT u.* FROM users u INNER JOIN orgs USING (org_id)')).toStrictEqual([
      {
        message: 'USING is not supported',
        source: 'SELECT u.* FROM users u INNER JOIN orgs USING (org_id)'
      }
    ]);
  });

  test('selecting connection value', () => {
    expect(compilationErrorsForSingleStream("SELECT u.*, auth.parameter('x') FROM users u;")).toStrictEqual([
      {
        message: 'This attempts to sync a connection parameter. Only values from the source database can be synced.',
        source: "auth.parameter('x')"
      }
    ]);
  });

  test('selecting from multiple tables', () => {
    expect(
      compilationErrorsForSingleStream(
        'SELECT u.*, orgs.* FROM users u INNER JOIN orgs ON u.id = auth.user_id() AND u.org = orgs.id'
      )
    ).toStrictEqual([
      {
        message: "Sync streams can only select from a single table, and this one already selects from 'users'.",
        source: 'orgs.*'
      }
    ]);
  });

  test('subexpressions from different rows', () => {
    expect(
      compilationErrorsForSingleStream(
        "SELECT u.* FROM users u INNER JOIN orgs WHERE u.name || orgs.name = subscription.parameter('a')"
      )
    ).toStrictEqual([
      {
        message:
          "This expression already references 'users', so it can't also reference data from this row unless the two are compared with an equals operator.",
        source: 'orgs.name'
      }
    ]);
  });

  test('ambigious reference', () => {
    expect(
      compilationErrorsForSingleStream('SELECT u.* FROM users u INNER JOIN orgs ON u.org = orgs.id WHERE is_public')
    ).toStrictEqual([
      {
        message: 'Invalid unqualified reference since multiple tables are in scope',
        source: 'is_public'
      }
    ]);
  });

  test('table that has not been added', () => {
    expect(compilationErrorsForSingleStream('SELECT users.* FROM orgs;')).toStrictEqual([
      {
        message: "Table 'users' has not been added in a FROM clause here.",
        source: 'users.*'
      },
      {
        message: 'Must have a result column selecting from a table',
        source: 'SELECT users.* FROM orgs'
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

  test('full join', () => {
    expect(compilationErrorsForSingleStream('select i.* from issues i FULL JOIN users u')).toStrictEqual([
      { message: 'FULL JOIN is not supported', source: 'select i.* from issues i FULL JOIN users u' }
    ]);
  });

  test('subquery star', () => {
    expect(compilationErrorsForSingleStream('select * from users where org in (select * from orgs)')).toStrictEqual([
      { message: 'Must return a single expression column', source: 'select * from orgs' }
    ]);
  });

  test('filter star', () => {
    expect(compilationErrorsForSingleStream('select * from users where users.*')).toStrictEqual([
      { message: '* columns are not supported here', source: 'users.*' }
    ]);
  });

  test('request parameter wrong count', () => {
    expect(
      compilationErrorsForSingleStream("select * from users where id = auth.parameter('foo', 'bar')")
    ).toStrictEqual([{ message: 'Expected a single argument here', source: 'auth.parameter' }]);
  });

  test('user_id on wrong schema', () => {
    expect(compilationErrorsForSingleStream('select * from users where id = subscription.user_id()')).toStrictEqual([
      { message: '.user_id() is only available on auth schema', source: 'subscription.user_id' }
    ]);
  });

  test('unknown request function', () => {
    expect(compilationErrorsForSingleStream('select * from users where id = subscription.whatever()')).toStrictEqual([
      { message: 'Unknown request function', source: 'subscription.whatever' }
    ]);
  });
});
