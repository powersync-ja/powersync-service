import { CompatibilityContext, CompatibilityOption } from './compatibility.js';
import { SqliteRow, SqliteValue } from './types.js';
import { jsonValueToSqlite } from './utils.js';

export interface TableValuedFunction {
  readonly name: string;
  call: (args: SqliteValue[]) => SqliteRow[];
  detail: string;
  documentation: string;
}

function jsonEachImplementation(fixedJsonBehavior: boolean): TableValuedFunction {
  return {
    name: 'json_each',
    call(args: SqliteValue[]) {
      if (args.length != 1) {
        throw new Error(`json_each expects 1 argument, got ${args.length}`);
      }
      const valueString = args[0];
      if (valueString === null) {
        return [];
      } else if (typeof valueString !== 'string') {
        throw new Error(`Expected json_each to be called with a string, got ${valueString}`);
      }
      let value: unknown;
      try {
        // FIXME: This should use JsonBig to parse integers as bigints.
        value = JSON.parse(valueString);
      } catch (e) {
        throw new Error('Expected JSON string');
      }

      if (Array.isArray(value)) {
        return value.map((v, key) => ({
          key,
          value: jsonValueToSqlite(fixedJsonBehavior, v)
        }));
      }

      if (typeof value == 'object' && value != null) {
        return Object.entries(value).map(([key, v]) => ({
          key,
          value: jsonValueToSqlite(fixedJsonBehavior, v)
        }));
      }

      return [{ key: null, value: jsonValueToSqlite(fixedJsonBehavior, value) }];
    },
    detail: 'Each element of a JSON value',
    documentation: 'Returns each element of a JSON array or object as a separate row.'
  };
}

export function generateTableValuedFunctions(compatibility: CompatibilityContext): Record<string, TableValuedFunction> {
  return { json_each: jsonEachImplementation(compatibility.isEnabled(CompatibilityOption.fixedJsonExtract)) };
}
