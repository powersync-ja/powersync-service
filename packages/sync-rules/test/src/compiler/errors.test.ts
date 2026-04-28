import { describe, expect, test } from 'vitest';
import { SqlSyncRules } from '../../../src/SqlSyncRules.js';
import { SourceTableDefinition, StaticSchema } from '../../../src/StaticSchema.js';
import { DEFAULT_TAG } from '../../../src/TablePattern.js';
import { SourceSchema } from '../../../src/types.js';
import { compilationErrorsForSingleStream, yamlToSyncPlan } from './utils.js';

function expectSingleErrorSource(yaml: string, source: string) {
  const { errors } = SqlSyncRules.fromYaml(yaml, { throwOnError: false, defaultSchema: 'test_schema' });
  const sources = errors.map((e) => yaml.substring(e.location.start, e.location.end));
  expect(sources).toHaveLength(1);
  expect(sources[0]).toEqual(source);
}

describe('compilation errors', () => {
  test('parsing error in query', () => {
    const [errors] = yamlToSyncPlan(`
config:
  edition: 3

streams:
  stream:
    query: invalid syntax
`);

    expect(errors).toHaveLength(1);
    const [error] = errors;
    expect(error.source).toStrictEqual('syntax');
  });

  test('parsing error in CTE', () => {
    const [errors] = yamlToSyncPlan(`
config:
  edition: 3

streams:
  stream:
    with:
      org_of_user: invalid syntax
    query: SELECT * FROM projects
`);

    expect(errors).toHaveLength(1);
    const [error] = errors;
    expect(error.source).toStrictEqual('syntax');
  });

  test('missing query', () => {
    const [errors] = yamlToSyncPlan(`
config:
  edition: 3

streams:
  stream:
    with:
      foo: SELECT id FROM users
`);

    expect(errors).toStrictEqual([
      {
        message: 'One of `queries` or `query` must be given.',
        source: `with:
      foo: SELECT id FROM users
`
      }
    ]);
  });

  test('not a select statement', () => {
    expect(compilationErrorsForSingleStream("INSERT INTO users (id) VALUES ('foo')")).toStrictEqual([
      {
        message: 'Expected a SELECT statement',
        source: "INSERT INTO users (id) VALUES ('foo'"
      }
    ]);
  });

  test('not selecting from anything', () => {
    expect(compilationErrorsForSingleStream('SELECT 1 AS a, 2 AS b, 3 AS c')).toStrictEqual([
      {
        message: 'Must have a result column selecting from a table',
        source: 'SELECT 1 AS a, 2 AS b, 3 AS c'
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

  test('selecting column from table-valued function', () => {
    expect(compilationErrorsForSingleStream("SELECT value FROM json_each(auth.parameter('x'))")).toStrictEqual([
      {
        message: 'Sync streams can only select from actual tables',
        source: 'value'
      },
      {
        message: 'Must have a result column selecting from a table',
        source: "SELECT value FROM json_each(auth.parameter('x'))"
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
    expect(compilationErrorsForSingleStream("SELECT u.*, auth.parameter('x') AS p FROM users u;")).toStrictEqual([
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

  test('partitioning table-valued result set', () => {
    // This is kind of an implementation detail and we could be smarter in querier_graph.ts, but currently we can't have
    // table-valued functions of request data in the middle of a parameter chain. At least we want a decent error
    // message.
    expect(
      compilationErrorsForSingleStream(
        `select comments.* from comments, json_each(connection.parameter('items')), user_access WHERE comments.item_id = json_each.value AND user_access.item_id = json_each.value AND user_access.user_id = auth.user_id()`
      )
    ).toStrictEqual([
      {
        message:
          "This table-valued function depends on request data and can't be partitioned. If possible, try rewriting the query to not use = operators on this function and multiple other tables.",
        source: `json_each(connection.parameter('items'))`
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

  test('warns about missing alias', () => {
    expect(compilationErrorsForSingleStream('select id, lower(name) from users')).toStrictEqual([
      {
        message: 'The name of this column is unspecified, consider adding an alias.',
        source: 'lower(name)',
        isWarning: true
      }
    ]);

    // Should not warn for subqueries with fixed column names.
    expect(
      compilationErrorsForSingleStream(
        'select id, name from (select id, lower(name) from users) as my_table (id, name)'
      )
    ).toStrictEqual([]);
  });

  test('error location', () => {
    // Verify that error locations are correctly mapped back to the raw YAML source for all
    // scalar types. Each query has a syntax error at the word 'broken'.
    const [errors] = yamlToSyncPlan(`
config:
  edition: 3
streams:
  foo:
    queries:
      - SELECT * FROM users WHERE something is broken
      - 'SELECT * FROM users WHERE something is broken'
      - "SELECT * FROM users WHERE something is broken"
      - SELECT * FROM users
        WHERE something is broken
      - |
          SELECT * FROM users
          WHERE something is broken
      - >
          SELECT * FROM users WHERE something is broken
`);

    expect(errors).toHaveLength(6);
    for (const error of errors) {
      expect(error.source).toEqual('broken');
    }
  });

  test('error location: double-quoted escape sequences', () => {
    const [errors] = yamlToSyncPlan(`
config:
  edition: 3
streams:
  foo:
    queries:
      - "SELECT * FROM users\\nWHERE something is broken"
      - "SELECT * FROM \\x75sers WHERE something is broken"
      - "SELECT * FROM \\u0075sers WHERE something is broken"
      - "SELECT * FROM \\U00000075sers WHERE something is broken"
`);

    expect(errors).toHaveLength(4);
    for (const error of errors) {
      expect(error.source).toEqual('broken');
    }
  });

  test('error location: single-quoted escape sequences', () => {
    const [errors] = yamlToSyncPlan(`
config:
  edition: 3
streams:
  foo:
    queries:
      - 'SELECT * FROM users WHERE ''something'' is broken'
`);

    expect(errors).toHaveLength(1);
    expect(errors[0].source).toEqual('broken');
  });

  test('error location: BLOCK_LITERAL with initial newline', () => {
    expectSingleErrorSource(
      `
config:
  edition: 3
streams:
  foo:
    queries:
      - |

          SELECT * FROM users
          WHERE something is broken
`,
      'broken'
    );
  });

  test('error location: BLOCK_FOLDED with initial newline', () => {
    expectSingleErrorSource(
      `
config:
  edition: 3
streams:
  foo:
    queries:
      - >

          SELECT * FROM users
          WHERE something is broken
`,
      'broken'
    );
  });

  test('error location: BLOCK_FOLDED with blank lines', () => {
    expectSingleErrorSource(
      `
config:
  edition: 3
streams:
  foo:
    queries:
      - >
          SELECT * FROM users


          WHERE something is broken
`,
      'broken'
    );
  });

  test('error location: plain multiline scalar with blank lines', () => {
    expectSingleErrorSource(
      `
config:
  edition: 3
streams:
  foo:
    queries:
      - SELECT * FROM users


        WHERE something is broken
`,
      'broken'
    );
  });

  test('error location: double-quoted legacy scalar', () => {
    expectSingleErrorSource(
      `
bucket_definitions:
  foo:
    data:
      - "SELECT * FROM users\\nWHERE something is broken"
`,
      'broken'
    );
  });

  test('error location: BLOCK_FOLDED with more-indented line', () => {
    // A more-indented line (extra spaces beyond base indent) must keep its surrounding \n literal.
    // The error at "broken" must still map back correctly through the non-folded boundaries.
    expectSingleErrorSource(
      `
config:
  edition: 3
streams:
  foo:
    queries:
      - >
          SELECT * FROM users
            WHERE 1=1
          AND something is broken
`,
      'broken'
    );
  });

  test('error location: multiline double-quoted scalar', () => {
    expectSingleErrorSource(
      `
config:
  edition: 3
streams:
  foo:
    queries:
      - "SELECT *
        FROM users
        WHERE something is broken"
`,
      'broken'
    );
  });

  test('error location: multiline single-quoted scalar', () => {
    expectSingleErrorSource(
      `
config:
  edition: 3
streams:
  foo:
    queries:
      - 'SELECT * FROM users
        WHERE something is broken'
`,
      'broken'
    );
  });

  test('error location: double-quoted scalar with escaped line break', () => {
    // \ + newline + leading whitespace → nothing (no space inserted)
    expectSingleErrorSource(
      `
config:
  edition: 3
streams:
  foo:
    queries:
      - "SELECT * FROM users \
        WHERE something is broken"
`,
      'broken'
    );
  });

  test('error location: double-quoted scalar with blank lines', () => {
    expectSingleErrorSource(
      `
config:
  edition: 3
streams:
  foo:
    queries:
      - "SELECT * FROM users


        WHERE something is broken"
`,
      'broken'
    );
  });

  describe('schema errors', () => {
    function schemaFromTables(...tables: SourceTableDefinition[]): StaticSchema {
      return new StaticSchema([{ tag: DEFAULT_TAG, schemas: [{ name: 'test_schema', tables }] }]);
    }

    function compilationErrorsWithSchema(schema: SourceSchema, sql: string) {
      const [errors] = yamlToSyncPlan(
        `
config:
  edition: 3

streams:
  stream:
    query: ${sql}
        `,
        { defaultSchema: 'test_schema', schema }
      );

      return errors;
    }

    test('unknown tables', () => {
      expect(compilationErrorsWithSchema(schemaFromTables(), 'SELECT * FROM users')).toStrictEqual([
        {
          message: 'This table could not be found in the source schema.',
          source: 'users',
          isWarning: true
        }
      ]);

      // We don't want to warn on columns too if the table couldn't be resolved.
      expect(compilationErrorsWithSchema(schemaFromTables(), 'SELECT id, name FROM users')).toStrictEqual([
        {
          message: 'This table could not be found in the source schema.',
          source: 'users',
          isWarning: true
        }
      ]);
    });

    test('unknown column', () => {
      expect(
        compilationErrorsWithSchema(
          schemaFromTables({
            name: 'users',
            columns: [
              { name: 'id', pg_type: 'uuid' },
              { name: 'name', pg_type: 'text' }
            ]
          }),
          'SELECT id, name, does_not_exist FROM users'
        )
      ).toStrictEqual([
        {
          message: 'Column not found.',
          source: 'does_not_exist',
          isWarning: true
        }
      ]);
    });
  });

  test('does not allow bucket_definitions', () => {
    const [errors] = yamlToSyncPlan(`
config:
  edition: 3

streams:
  foo:
    query: SELECT * FROM users

bucket_definitions:
  a:
    data:
      - SELECT * FROM users
`);

    expect(errors).toHaveLength(1);
    const [error] = errors;
    expect(error.message).toContain("'bucket_definitions' are not supported by the new compiler.");
    expect(error.source).toStrictEqual(`a:
    data:
      - SELECT * FROM users
`);
  });

  describe('warns about potentially dangerous queries', () => {
    function checkDangerousQueryWarning(query: string, expectedError: boolean) {
      const [errors] = yamlToSyncPlan(`
config:
  edition: 3

streams:
  foo:
    query: ${query}
`);

      if (expectedError) {
        expect(errors).toHaveLength(1);
        const [error] = errors;
        expect(error.message).toContain('this is unsuitable for authorization');
        expect(error.isWarning).toBeTruthy();

        // Should not report the dangerous queries warning with the tag applied.
        const [errorsWithOptIn] = yamlToSyncPlan(`
config:
  edition: 3

streams:
  foo:
    accept_potentially_dangerous_queries: true
    query: ${query}
`);
        expect(errorsWithOptIn).toStrictEqual([]);
      } else {
        expect(errors).toStrictEqual([]);
      }
    }

    // These examples are taken from SqlParameterQuery.usesDangerousRequestParameters
    test('safe', () => {
      checkDangerousQueryWarning('SELECT * FROM users WHERE user_id = auth.user_id()', false);
      checkDangerousQueryWarning(
        `SELECT * FROM notes WHERE org = auth.parameter('org') AND project = subscription.parameter('project')`,
        false
      );
      checkDangerousQueryWarning(
        `SELECT * FROM notes WHERE org = auth.parameter('org') AND project = connection.parameter('project')`,
        false
      );
      checkDangerousQueryWarning('SELECT * FROM users', false);

      checkDangerousQueryWarning(`SELECT * FROM projects WHERE is_public OR owner = auth.user_id()`, false);
    });

    test('dangerous', () => {
      checkDangerousQueryWarning(`SELECT * FROM projects WHERE id = subscription.parameter('project')`, true);
      checkDangerousQueryWarning(
        `SELECT * FROM projects WHERE id = subscription.parameter('project') AND auth.parameter('role') = 'authenticated'`,
        true
      );
      checkDangerousQueryWarning(`SELECT * FROM categories WHERE connection.parameters('include_categories')`, true);

      checkDangerousQueryWarning(
        `SELECT * FROM projects WHERE id = subscription.parameter('id') OR owner = auth.user_id()`,
        true
      );
    });
  });

  test('avoids OOM when transforming to disjunctive normal form', () => {
    let terms = '';
    // Generate a filter that would require 2^60 nodes to represent in DNF.
    for (let i = 0; i < 60; i++) {
      if (i != 0) terms += ' AND ';

      terms += `(subscription.parameter('skip_cond_${i}') OR tbl.column${i} = subscription.parameter('filter_${i}'))`;
    }

    const errors = compilationErrorsForSingleStream(`SELECT * FROM tbl WHERE ${terms}`);
    expect(errors).toMatchObject([
      {
        message:
          'For Sync Streams, inner OR operators need to be moved up to be top-level filters. Applying that to this query results in too many inner nodes.'
      }
    ]);
  });

  test('restricts bucket sources per stream', () => {
    function generateFilter(table: string) {
      let terms = '';

      for (let i = 0; i < 6; i++) {
        if (i != 0) terms += ' AND ';

        terms += `(subscription.parameter('skip_${table}_${i}') OR ${table}.column${i} = subscription.parameter('filter_${table}_${i}'))`;
      }

      return terms;
    }

    const [errors, _] = yamlToSyncPlan(`
config:
  edition: 3

streams:
  manybuckets:
    accept_potentially_dangerous_queries: true
    queries:
      - SELECT * FROM tbl0 WHERE ${generateFilter('tbl0')}
      - SELECT * FROM tbl1 WHERE ${generateFilter('tbl1')}
`);

    expect(errors).toMatchObject([
      {
        message:
          'This stream defines too many buckets (128, at most 100 are allowed). Try splitting queries into separate streams or move inner OR operators in filters to separate queries.'
      }
    ]);
  });

  test('arrays where literals are expected', () => {
    const [errors, _] = yamlToSyncPlan(`
config:
  edition: 3

streams:
  manybuckets:
    with:
      foo: # should not be an array
        - SELECT 1 as bar
    query: # should also not be an array (unless queries is used as a key)
      - SELECT * FROM tbl WHERE id IN foo
`);

    expect(errors).toStrictEqual([
      { message: 'Expected a scalar value here.', source: '- SELECT 1 as bar\n' },
      { message: 'Expected a scalar value here.', source: '- SELECT * FROM tbl WHERE id IN foo\n' }
    ]);
  });
});
