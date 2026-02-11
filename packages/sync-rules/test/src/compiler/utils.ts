import { expect } from 'vitest';
import {
  deserializeSyncPlan,
  getLocation,
  ParsingErrorListener,
  PrecompiledSyncConfig,
  serializeSyncPlan,
  SqlSyncRules,
  SyncPlan,
  SyncStreamsCompiler,
  SyncStreamsCompilerOptions
} from '../../../src/index.js';

interface TranslationError {
  message: string;
  source: string;
  isWarning?: boolean;
}

export function compileSingleStreamAndSerialize(...sql: string[]): unknown {
  const [errors, plan] = compileSingleStream(...sql);
  expect(errors).toStrictEqual([]);
  return serializeSyncPlan(plan);
}

export function compilationErrorsForSingleStream(...sql: string[]): TranslationError[] {
  return compileSingleStream(...sql)[0];
}

export function compileAndSerialize(yaml: string): unknown {
  const plan = compileToSyncPlanWithoutErrors(yaml);
  return serializeSyncPlan(plan);
}

export function compileToSyncPlanWithoutErrors(yaml: string): SyncPlan {
  const [errors, plan] = yamlToSyncPlan(yaml);
  expect(errors).toStrictEqual([]);
  return plan;
}

function compileSingleStream(...sql: string[]): [TranslationError[], SyncPlan] {
  const compiler = new SyncStreamsCompiler({ defaultSchema: 'test_schema' });
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

  const builder = compiler.stream({ name: 'stream', isSubscribedByDefault: true, priority: 3 });
  for (const query of sql) {
    builder.addQuery(query, errorListenerOnSql(query));
  }
  builder.finish();

  const originalPlan = compiler.output.toSyncPlan();
  // Add a serialization roundtrip to ensure sync plans are correctly evaluated even after being deserialized.
  const afterSerializationRoundtrip = deserializeSyncPlan(JSON.parse(JSON.stringify(serializeSyncPlan(originalPlan))));

  return [errors, afterSerializationRoundtrip];
}

export function yamlToSyncPlan(
  source: string,
  options: SyncStreamsCompilerOptions = { defaultSchema: 'test_schema' }
): [TranslationError[], SyncPlan] {
  const { config, errors } = SqlSyncRules.fromYaml(source, {
    throwOnError: false,
    ...options
  });

  const mappedErrors = errors.map((e) => {
    const error: TranslationError = { message: e.message, source: source.substring(e.location.start, e.location.end) };
    if ('type' in e && e.type == 'warning') {
      error.isWarning = true;
    }

    return error;
  });

  const originalPlan = (config as PrecompiledSyncConfig).plan;
  const afterSerializationRoundtrip = deserializeSyncPlan(JSON.parse(JSON.stringify(serializeSyncPlan(originalPlan))));

  return [mappedErrors, afterSerializationRoundtrip];
}
