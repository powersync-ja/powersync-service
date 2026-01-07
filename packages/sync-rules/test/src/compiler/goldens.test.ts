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

      const [stmt] = parse(sql);
      const parser = new StreamQueryParser({
        originalText: sql,
        errors: {
          report(message, location) {
            console.log(message, location);
          }
        }
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
