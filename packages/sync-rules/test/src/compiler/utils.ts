import { parse } from 'pgsql-ast-parser';
import { ParsingErrorListener, SyncStreamCompiler } from '../../../src/compiler/compiler.js';
import { StreamQueryParser } from '../../../src/compiler/parser.js';
import { QuerierGraphBuilder } from '../../../src/compiler/querier_graph.js';
import { getLocation } from '../../../src/errors.js';
import { SyncPlan } from '../../../src/sync_plan/plan.js';
import { serializeSyncPlan } from '../../../src/sync_plan/serialize.js';
import { expect } from 'vitest';

// TODO: Replace with parsing from yaml once we support that
interface SyncStreamInput {
  name: string;
  queries: string[];
}

interface TranslationError {
  message: string;
  source: string;
}

export function compileSingleStreamAndSerialize(...sql: string[]): unknown {
  return compileAndSerialize([
    {
      name: 'stream',
      queries: sql
    }
  ]);
}

export function compileAndSerialize(inputs: SyncStreamInput[]): unknown {
  const plan = compileToSyncPlanWithoutErrors(inputs);
  return serializeSyncPlan(plan);
}

export function compileToSyncPlanWithoutErrors(inputs: SyncStreamInput[]): SyncPlan {
  const [errors, plan] = compileToSyncPlan(inputs);
  expect(errors).toStrictEqual([]);
  return plan;
}

export function compilationErrorsForSingleStream(...sql: string[]): TranslationError[] {
  return compileToSyncPlan([
    {
      name: 'stream',
      queries: sql
    }
  ])[0];
}

export function compilationErrors(inputs: SyncStreamInput[]): TranslationError[] {
  return compileToSyncPlan(inputs)[0];
}

export function compileToSyncPlan(inputs: SyncStreamInput[]): [TranslationError[], SyncPlan] {
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

  return [errors, compiler.output.toSyncPlan()];
}
