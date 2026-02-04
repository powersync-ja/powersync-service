import { expect } from 'vitest';
import {
  deserializeSyncPlan,
  getLocation,
  ParsingErrorListener,
  serializeSyncPlan,
  SyncPlan,
  SyncStreamsCompiler,
  SyncStreamsCompilerOptions
} from '../../../src/index.js';

// TODO: Replace with parsing from yaml once we support that
export interface SyncStreamInput {
  name: string;
  queries: string[];
  ctes?: Record<string, string>;
}

interface TranslationError {
  message: string;
  source: string;
  isWarning?: boolean;
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

export function compileToSyncPlan(
  inputs: SyncStreamInput[],
  options: SyncStreamsCompilerOptions = { defaultSchema: 'test_schema' }
): [TranslationError[], SyncPlan] {
  const compiler = new SyncStreamsCompiler(options);
  const errors: TranslationError[] = [];

  function errorListenerOnSql(sql: string): ParsingErrorListener {
    return {
      report(message, location, options) {
        const resolved = getLocation(location);
        const error: TranslationError = { message, source: sql.substring(resolved?.start ?? 0, resolved?.end) };
        if (options?.isWarning) {
          error.isWarning = true;
        }

        errors.push(error);
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

  const originalPlan = compiler.output.toSyncPlan();
  // Add a serialization roundtrip to ensure sync plans are correctly evaluated even after being deserialized.
  const afterSerializationRoundtrip = deserializeSyncPlan(JSON.parse(JSON.stringify(serializeSyncPlan(originalPlan))));

  return [errors, afterSerializationRoundtrip];
}
