import { SqliteJsonValue, SqliteRow, SqliteValue } from './types.js';
import { jsonValueToSqlite } from './utils.js';

export interface TableValuedFunction {
  readonly name: string;
  call: (args: SqliteValue[]) => SqliteRow[];
  detail: string;
  documentation: string;
}

export const JSON_EACH: TableValuedFunction = {
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
    let values: SqliteJsonValue[] = [];
    try {
      values = JSON.parse(valueString);
    } catch (e) {
      throw new Error('Expected JSON string');
    }
    if (!Array.isArray(values)) {
      throw new Error('Expected an array');
    }

    return values.map((v) => {
      return {
        value: jsonValueToSqlite(v)
      };
    });
  },
  detail: 'Each element of a JSON array',
  documentation: 'Returns each element of a JSON array as a separate row.'
};

export const TABLE_VALUED_FUNCTIONS: Record<string, TableValuedFunction> = {
  json_each: JSON_EACH
};
