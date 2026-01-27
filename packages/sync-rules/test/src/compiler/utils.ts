import { expect } from 'vitest';
import {
  getLocation,
  ParsingErrorListener,
  serializeSyncPlan,
  SyncPlan,
  SyncStreamsCompiler
} from '../../../src/index.js';

// TODO: Replace with parsing from yaml once we support that
interface SyncStreamInput {
  name: string;
  queries: string[];
  ctes?: Record<string, string>;
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

export function compileToSyncPlan(inputs: SyncStreamInput[]): [TranslationError[], SyncPlan] {
  const compiler = new SyncStreamsCompiler('test_schema');
  const errors: TranslationError[] = [];

  function errorListenerOnSql(sql: string): ParsingErrorListener {
    return {
      report(message, location) {
        const resolved = getLocation(location);
        errors.push({ message, source: sql.substring(resolved?.start ?? 0, resolved?.end) });
      }
    };
  }

  for (const input of inputs) {
    const builder = compiler.stream({ name: input.name, isSubscribedByDefault: true, priority: 3 });

    for (const [name, sql] of Object.entries(input.ctes ?? {})) {
      const cte = compiler.commonTableExpression(sql, errorListenerOnSql(sql));
      if (cte) {
        builder.registerCommonTableExpression(name, cte);
      }
    }

    for (const sql of input.queries) {
      builder.addQuery(sql, errorListenerOnSql(sql));
    }

    builder.finish();
  }

  return [errors, compiler.output.toSyncPlan()];
}
