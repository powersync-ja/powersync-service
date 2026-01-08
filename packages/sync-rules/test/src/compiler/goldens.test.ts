import { describe, expect, test } from 'vitest';
import { StreamQueryParser } from '../../../src/compiler/parser.js';
import { parse } from 'pgsql-ast-parser';
import { ParsingErrorListener, SyncStreamCompiler } from '../../../src/compiler/compiler.js';
import { QuerierGraphBuilder } from '../../../src/compiler/querier_graph.js';
import { getLocation } from '../../../src/errors.js';
import { SyncPlan } from '../../../src/sync_plan/plan.js';
import { serializeSyncPlan } from '../../../src/sync_plan/serialize.js';

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
});

// TODO: Replace with parsing from yaml once we support that
interface SyncStreamInput {
  name: string;
  queries: string[];
}

interface TranslationError {
  message: string;
  source: string;
}

function compileSingleStreamAndSerialize(...sql: string[]): unknown {
  return compileAndSerialize([
    {
      name: 'stream',
      queries: sql
    }
  ]);
}

function compileAndSerialize(inputs: SyncStreamInput[]): unknown {
  const plan = compileToSyncPlan(inputs);
  return serializeSyncPlan(plan);
}

function compileToSyncPlan(inputs: SyncStreamInput[]): SyncPlan {
  const compiler = new SyncStreamCompiler();
  const errors: TranslationError[] = [];

  for (const input of inputs) {
    const builder = new QuerierGraphBuilder(compiler, { name: input.name, isSubscribedByDefault: true, priority: 3 });

    for (const sql of input.queries) {
      const listener: ParsingErrorListener = {
        report(message, location) {
          const resolved = getLocation(location);
          errors.push({ message, source: sql.substring(resolved?.start ?? 0, resolved?.end) });
        }
      };

      const [stmt] = parse(sql, { locationTracking: true });
      const parser = new StreamQueryParser({
        originalText: sql,
        errors: listener
      });
      const query = parser.parse(stmt);
      if (query) {
        builder.process(query, listener);
      }
    }

    builder.finish();
  }

  expect(errors).toStrictEqual([]);
  return compiler.output.toSyncPlan();
}
