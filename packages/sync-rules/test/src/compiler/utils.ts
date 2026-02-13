import { expect } from 'vitest';
import {
  compileSyncPlanRust,
  deserializeSyncPlan,
  getLocation,
  ParsingErrorListener,
  PrecompiledSyncConfig,
  serializeSyncPlan,
  SqlSyncRules,
  SyncPlan,
  SyncRulesOptions,
  SyncStreamsCompiler
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
  assertRustParity(toSingleStreamYaml(sql), errors, serializeSyncPlan(afterSerializationRoundtrip));

  return [errors, afterSerializationRoundtrip];
}

export function yamlToSyncPlan(
  source: string,
  options: SyncRulesOptions = { defaultSchema: 'test_schema' }
): [TranslationError[], SyncPlan] {
  const { config, errors } = SqlSyncRules.fromYaml(source, {
    throwOnError: false,
    allowNewSyncCompiler: true,
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
  if (options.defaultSchema == null || options.defaultSchema == 'test_schema') {
    assertRustParity(source, mappedErrors, serializeSyncPlan(afterSerializationRoundtrip));
  }

  return [mappedErrors, afterSerializationRoundtrip];
}

function assertRustParity(yaml: string, jsErrors: TranslationError[], serializedJsPlan: unknown) {
  let rustPlan: unknown;
  try {
    rustPlan = compileSyncPlanRust(yaml, { defaultSchema: 'test_schema' });
  } catch {
    return;
  }

  if (jsErrors.length != 0) {
    return;
  }

  expect(withZeroHashes(rustPlan)).toStrictEqual(withZeroHashes(serializedJsPlan));
}

function withZeroHashes<T>(value: T): T {
  const cloned = structuredClone(value);
  zeroHashes(cloned as unknown);
  return cloned;
}

function zeroHashes(value: unknown) {
  if (Array.isArray(value)) {
    for (const element of value) {
      zeroHashes(element);
    }
    return;
  }

  if (value && typeof value == 'object') {
    const obj = value as Record<string, unknown>;
    for (const [key, nested] of Object.entries(obj)) {
      if (nested === undefined) {
        delete obj[key];
      } else if (key == 'hash' && typeof nested == 'number') {
        obj[key] = 0;
      } else if ((key == 'outputTableName' || key == 'operand') && nested === null) {
        delete obj[key];
      } else {
        zeroHashes(nested);
      }
    }
  }
}

function toSingleStreamYaml(sql: string[]) {
  const queryBlock =
    sql.length == 1
      ? `    query: ${normalizeSql(sql[0])}\n`
      : `    queries:\n${sql.map((query) => `      - ${normalizeSql(query)}`).join('\n')}\n`;

  return `config:
  edition: 2
  sync_config_compiler: true
streams:
  stream:
    auto_subscribe: true
${queryBlock}`;
}

function normalizeSql(sql: string) {
  return sql
    .split('\n')
    .map((line) => line.trim())
    .filter((line) => line.length != 0)
    .join(' ');
}
