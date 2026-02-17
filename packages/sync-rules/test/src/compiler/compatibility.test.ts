import { describe, expect, test } from 'vitest';
import { compileSingleStreamAndSerialize } from './utils.js';

describe('old streams test', () => {
  // Testcases ported from streams.test.ts
  test('without filter', () => {
    expect(compileSingleStreamAndSerialize('SELECT * FROM comments')).toMatchSnapshot();
  });

  test('row condition', () => {
    expect(compileSingleStreamAndSerialize('SELECT * FROM comments WHERE length(content) > 5')).toMatchSnapshot();
  });

  test('stream parameter', () => {
    expect(
      compileSingleStreamAndSerialize("SELECT * FROM comments WHERE issue_id = subscription.parameter('id')")
    ).toMatchSnapshot();
  });

  test('row filter and stream parameter', () => {
    expect(
      compileSingleStreamAndSerialize(
        "SELECT * FROM comments WHERE length(content) > 5 AND issue_id = subscription.parameter('id')"
      )
    ).toMatchSnapshot();
  });

  describe('or', () => {
    test('parameter match or request condition', () => {
      expect(
        compileSingleStreamAndSerialize(
          "SELECT * FROM issues WHERE owner_id = auth.user_id() OR auth.parameter('is_admin')"
        )
      ).toMatchSnapshot();
    });

    test('parameter match or row condition', () => {
      expect(
        compileSingleStreamAndSerialize('SELECT * FROM issues WHERE owner_id = auth.user_id() OR LENGTH(name) = 3')
      ).toMatchSnapshot();
    });

    test('row condition or parameter condition', () => {
      expect(
        compileSingleStreamAndSerialize(
          "SELECT * FROM comments WHERE LENGTH(content) > 5 OR auth.parameter('is_admin')"
        )
      ).toMatchSnapshot();
    });

    test('row condition or row condition', () => {
      expect(
        compileSingleStreamAndSerialize(
          'SELECT * FROM comments WHERE LENGTH(content) > 5 OR json_array_length(tagged_users) > 1'
        )
      ).toMatchSnapshot();
    });

    test('request condition or request condition', () => {
      expect(
        compileSingleStreamAndSerialize("SELECT * FROM comments WHERE auth.parameter('a') OR auth.parameters() ->> 'b'")
      ).toMatchSnapshot();
    });

    test('subquery or token parameter', () => {
      expect(
        compileSingleStreamAndSerialize(
          "SELECT * FROM comments WHERE issue_id IN (SELECT id FROM issues WHERE owner_id = auth.user_id()) OR auth.parameter('is_admin')"
        )
      ).toMatchSnapshot();
    });
  });

  describe('normalization', () => {
    test('double negation', () => {
      expect(
        compileSingleStreamAndSerialize(
          'select * from comments where NOT (issue_id not in (select id from issues where owner_id = auth.user_id()))'
        )
      ).toMatchSnapshot();
    });

    test('negated or', () => {
      expect(
        compileSingleStreamAndSerialize(
          'select * from comments where not (length(content) = 5 OR issue_id not in (select id from issues where owner_id = auth.user_id()))'
        )
      ).toMatchSnapshot();
    });

    test('negated and', () => {
      expect(
        compileSingleStreamAndSerialize(
          'select * from comments where not (length(content) = 5 AND issue_id not in (select id from issues where owner_id = auth.user_id()))'
        )
      ).toMatchSnapshot();
    });

    test('distribute and', () => {
      expect(
        compileSingleStreamAndSerialize(
          `select * from comments where 
          (issue_id in (select id from issues where owner_id = auth.user_id())
            OR auth.parameter('is_admin'))
          AND
          LENGTH(content) > 2
        `
        )
      ).toMatchSnapshot();
    });
  });

  describe('in', () => {
    test('row value in subquery', () => {
      expect(
        compileSingleStreamAndSerialize(
          'SELECT * FROM comments WHERE issue_id IN (SELECT id FROM issues WHERE owner_id = auth.user_id())'
        )
      ).toMatchSnapshot();
    });

    test('parameter value in subquery', () => {
      expect(
        compileSingleStreamAndSerialize(
          'SELECT * FROM issues WHERE auth.user_id() IN (SELECT id FROM users WHERE is_admin)'
        )
      ).toMatchSnapshot();
    });

    test('two subqueries', () => {
      expect(
        compileSingleStreamAndSerialize(
          `SELECT * FROM users WHERE
            id IN (SELECT user_a FROM friends WHERE user_b = auth.user_id()) OR
            id IN (SELECT user_b FROM friends WHERE user_a = auth.user_id())`
        )
      ).toMatchSnapshot();
    });

    test('on parameter data', () => {
      expect(
        compileSingleStreamAndSerialize(
          "SELECT * FROM comments WHERE issue_id IN (subscription.parameters() -> 'issue_id')"
        )
      ).toMatchSnapshot();
    });

    test('on parameter data and table', () => {
      expect(
        compileSingleStreamAndSerialize(
          "SELECT * FROM comments WHERE issue_id IN (SELECT id FROM issues WHERE owner_id = auth.user_id()) AND label IN (subscription.parameters() -> 'labels')"
        )
      ).toMatchSnapshot();
    });

    test('parameter and auth match on same column', () => {
      expect(
        compileSingleStreamAndSerialize(
          "SELECT * FROM comments WHERE issue_id = subscription.parameter('issue') AND issue_id IN auth.parameter('issues')"
        )
      ).toMatchSnapshot();
    });
  });

  test('overlap', () => {
    expect(
      compileSingleStreamAndSerialize(
        'SELECT * FROM comments WHERE tagged_users && (SELECT user_a FROM friends WHERE user_b = auth.user_id())'
      )
    ).toMatchSnapshot();
  });

  test('OR in subquery', () => {
    // This used to be an error with the old sync streams compiler, but is supported now
    expect(
      compileSingleStreamAndSerialize(
        `select * from comments where issue_id in (select id from issues where owner_id = auth.user_id() or name = 'test')`
      )
    ).toMatchSnapshot();
  });

  test('nested subqueries', () => {
    // This used to be an error with the old sync streams compiler, but is supported now
    expect(
      compileSingleStreamAndSerialize(
        `select * from comments where issue_id in (select id from issues where owner_id in (select id from users where is_admin))`
      )
    ).toMatchSnapshot();
  });

  test('table alias', () => {
    expect(
      compileSingleStreamAndSerialize(
        'select * from account_member as "outer" where account_id in (select "inner".account_id from account_member as "inner" where "inner".id = auth.user_id())'
      )
    ).toMatchSnapshot();
  });
});
