import { JSONBig } from '@powersync/service-jsonbig';
import { SQLITE_FALSE, SQLITE_TRUE, sqliteBool, sqliteNot } from './sql_support.js';
import { SqliteValue } from './types.js';
import { jsonValueToSqlite } from './utils.js';
// Declares @syncpoint/wkx module
// This allows for consumers of this lib to resolve types correctly
/// <reference types="./wkx.d.ts" />
import wkx from '@syncpoint/wkx';
import { ExpressionType, SqliteType, TYPE_INTEGER } from './ExpressionType.js';
import * as uuid from 'uuid';

export const BASIC_OPERATORS = new Set<string>([
  '=',
  '!=',
  '<',
  '>',
  '<=',
  '>=',
  '+',
  '-',
  '*',
  '/',
  '||',
  'AND',
  'OR',
  'IS',
  'IS NOT'
]);

export interface FunctionParameter {
  name: string;
  type: ExpressionType;
  optional: boolean;
}

export interface SqlFunction {
  readonly debugName: string;
  call: (...args: SqliteValue[]) => SqliteValue;
  getReturnType(args: ExpressionType[]): ExpressionType;
}

export interface DocumentedSqlFunction extends SqlFunction {
  parameters: FunctionParameter[];
  detail: string;
  documentation?: string;
}

export function getOperatorFunction(op: string): SqlFunction {
  return {
    debugName: `operator${op}`,
    call(...args: SqliteValue[]) {
      return evaluateOperator(op, args[0], args[1]);
    },
    getReturnType(args) {
      return getOperatorReturnType(op, args[0], args[1]);
    }
  };
}

const upper: DocumentedSqlFunction = {
  debugName: 'upper',
  call(value: SqliteValue) {
    const text = castAsText(value);
    return text?.toUpperCase() ?? null;
  },
  parameters: [{ name: 'value', type: ExpressionType.ANY, optional: false }],
  getReturnType(args) {
    return ExpressionType.TEXT;
  },
  detail: 'Convert text to upper case'
};

const lower: DocumentedSqlFunction = {
  debugName: 'lower',
  call(value: SqliteValue) {
    const text = castAsText(value);
    return text?.toLowerCase() ?? null;
  },
  parameters: [{ name: 'value', type: ExpressionType.ANY, optional: false }],
  getReturnType(args) {
    return ExpressionType.TEXT;
  },
  detail: 'Convert text to lower case'
};

const substring: DocumentedSqlFunction = {
  debugName: 'substring',
  call(value: SqliteValue, start: SqliteValue, length?: SqliteValue) {
    const text = castAsText(value);
    if (text == null) {
      return null;
    }
    const startIndex = cast(start, 'integer') as bigint | null;
    if (startIndex == null) {
      return null;
    }
    if (length === null) {
      // Different from undefined in this case, to match SQLite behavior
      return null;
    }
    const castLength = cast(length ?? null, 'integer') as bigint | null;
    let realLength: number;
    if (castLength == null) {
      // undefined (not specified)
      realLength = text.length + 1; // +1 to account for the start = 0 special case
    } else {
      realLength = Number(castLength);
    }

    let realStart = 0;
    if (startIndex < 0n) {
      realStart = Math.max(0, text.length + Number(startIndex));
    } else if (startIndex == 0n) {
      // Weird special case
      realStart = 0;
      realLength -= 1;
    } else {
      realStart = Number(startIndex) - 1;
    }

    if (realLength < 0) {
      // Negative length means we return that many characters _before_
      // the start index.
      return text.substring(realStart + realLength, realStart);
    }

    return text.substring(realStart, realStart + realLength);
  },
  parameters: [
    { name: 'value', type: ExpressionType.TEXT, optional: false },
    { name: 'start', type: ExpressionType.INTEGER, optional: false },
    { name: 'length', type: ExpressionType.INTEGER, optional: true }
  ],
  getReturnType(args) {
    return ExpressionType.TEXT;
  },
  detail: 'Compute a substring',
  documentation: 'The start index starts at 1. If no length is specified, the remainder of the string is returned.'
};

const hex: DocumentedSqlFunction = {
  debugName: 'hex',
  call(value: SqliteValue) {
    const binary = castAsBlob(value);
    if (binary == null) {
      return '';
    }
    return Buffer.from(binary).toString('hex').toUpperCase();
  },
  parameters: [{ name: 'value', type: ExpressionType.ANY, optional: false }],
  getReturnType(args) {
    return ExpressionType.TEXT;
  },
  detail: 'Convert a blob to hex text'
};

const length: DocumentedSqlFunction = {
  debugName: 'length',
  call(value: SqliteValue) {
    if (value == null) {
      return null;
    } else if (value instanceof Uint8Array) {
      return BigInt(value.byteLength);
    } else {
      value = castAsText(value);
      return BigInt(value!.length);
    }
  },
  parameters: [{ name: 'value', type: ExpressionType.ANY, optional: false }],
  getReturnType(args) {
    return ExpressionType.INTEGER;
  },
  detail: 'Returns the length of a text or blob value'
};

const base64: DocumentedSqlFunction = {
  debugName: 'base64',
  call(value: SqliteValue) {
    const binary = castAsBlob(value);
    if (binary == null) {
      return '';
    }
    return Buffer.from(binary).toString('base64');
  },
  parameters: [{ name: 'value', type: ExpressionType.ANY, optional: false }],
  getReturnType(args) {
    return ExpressionType.TEXT;
  },
  detail: 'Convert a blob to base64 text'
};

const uuid_blob: DocumentedSqlFunction = {
  debugName: 'uuid_blob',
  call(value: SqliteValue) {
    const uuidText = castAsText(value);

    if (uuidText == null) {
      return null;
    }

    const isValid = uuid.validate(uuidText);

    if (!isValid) {
      throw new Error(`Cannot call uuid_blob on a non UUID value`);
    }

    return uuid.parse(uuidText);
  },
  parameters: [{ name: 'uuid', type: ExpressionType.TEXT, optional: false }],
  getReturnType(args) {
    return ExpressionType.BLOB;
  },
  detail: 'Convert the UUID string to bytes'
};

const fn_typeof: DocumentedSqlFunction = {
  debugName: 'typeof',
  call(value: SqliteValue) {
    return sqliteTypeOf(value);
  },
  parameters: [{ name: 'value', type: ExpressionType.ANY, optional: false }],
  getReturnType(args) {
    return ExpressionType.TEXT;
  },
  detail: 'Returns the SQLite type of a value',
  documentation: `Returns 'null', 'text', 'integer', 'real' or 'blob'.`
};

const ifnull: DocumentedSqlFunction = {
  debugName: 'ifnull',
  call(x: SqliteValue, y: SqliteValue) {
    if (x == null) {
      return y;
    } else {
      return x;
    }
  },
  parameters: [
    { name: 'x', type: ExpressionType.ANY, optional: false },
    { name: 'y', type: ExpressionType.ANY, optional: false }
  ],
  getReturnType(args) {
    if (args.length == 0) {
      return ExpressionType.NONE;
    } else if (args.length == 1) {
      return args[0];
    } else {
      return args[0].or(args[1]);
    }
  },
  detail: 'Returns the first non-null parameter'
};

// This is the same behavior as the iif function in SQLite
const iif: DocumentedSqlFunction = {
  debugName: 'iif',
  call(x: SqliteValue, y: SqliteValue, z: SqliteValue) {
    return sqliteBool(x) ? y : z;
  },
  parameters: [
    { name: 'x', type: ExpressionType.ANY, optional: false },
    { name: 'y', type: ExpressionType.ANY, optional: false },
    { name: 'z', type: ExpressionType.ANY, optional: false }
  ],
  getReturnType() {
    return ExpressionType.ANY;
  },
  detail: 'If x is true then returns y else returns z'
};

const json_extract: DocumentedSqlFunction = {
  debugName: 'json_extract',
  call(json: SqliteValue, path: SqliteValue) {
    return jsonExtract(json, path, 'json_extract');
  },
  parameters: [
    { name: 'json', type: ExpressionType.ANY, optional: false },
    { name: 'path', type: ExpressionType.ANY, optional: false }
  ],
  getReturnType(args) {
    return ExpressionType.ANY_JSON;
  },
  detail: 'Extract a JSON property'
};

const json_array_length: DocumentedSqlFunction = {
  debugName: 'json_array_length',
  call(json: SqliteValue, path?: SqliteValue) {
    if (path != null) {
      json = json_extract.call(json, path);
    }
    const jsonString = castAsText(json);
    if (jsonString == null) {
      return null;
    }

    const jsonParsed = JSONBig.parse(jsonString);
    if (!Array.isArray(jsonParsed)) {
      return 0n;
    }
    return BigInt(jsonParsed.length);
  },
  parameters: [
    { name: 'json', type: ExpressionType.ANY, optional: false },
    { name: 'path', type: ExpressionType.ANY, optional: true }
  ],
  getReturnType(args) {
    return ExpressionType.INTEGER;
  },
  detail: 'Returns the length of a JSON array'
};

const json_valid: DocumentedSqlFunction = {
  debugName: 'json_valid',
  call(json: SqliteValue) {
    const jsonString = castAsText(json);
    if (jsonString == null) {
      return SQLITE_FALSE;
    }
    try {
      JSONBig.parse(jsonString);
      return SQLITE_TRUE;
    } catch (e) {
      return SQLITE_FALSE;
    }
  },
  parameters: [{ name: 'json', type: ExpressionType.ANY, optional: false }],
  getReturnType(args) {
    return ExpressionType.INTEGER;
  },
  detail: 'Checks whether JSON text is valid',
  documentation: 'Returns 1 if valid, 0 if invalid'
};

const json_keys: DocumentedSqlFunction = {
  debugName: 'json_keys',
  call(json: SqliteValue) {
    const jsonString = castAsText(json);
    if (jsonString == null) {
      return null;
    }

    const jsonParsed = JSONBig.parse(jsonString);
    if (typeof jsonParsed != 'object') {
      throw new Error(`Cannot call json_keys on a scalar`);
    } else if (Array.isArray(jsonParsed)) {
      throw new Error(`Cannot call json_keys on an array`);
    }
    const keys = Object.keys(jsonParsed as {});
    // Keys are always strings, safe to use plain JSON.
    return JSON.stringify(keys);
  },
  parameters: [{ name: 'json', type: ExpressionType.ANY, optional: false }],
  getReturnType(args) {
    // TODO: proper nullable types
    return ExpressionType.TEXT;
  },
  detail: 'Returns the keys of a JSON object as a JSON array'
};

const unixepoch: DocumentedSqlFunction = {
  debugName: 'unixepoch',
  call(value?: SqliteValue, specifier?: SqliteValue, specifier2?: SqliteValue) {
    if (value == null) {
      return null;
    }
    let flags: ParseDateFlags = {
      unixepoch: false,
      subsecond: false
    };
    if (specifier == null) {
    } else if (specifier == 'unixepoch') {
      flags.unixepoch = true;
      if (specifier2 == null) {
      } else if (specifier2 == 'subsec' || specifier2 == 'subsecond') {
        flags.subsecond = true;
      } else {
        return null;
      }
    } else if (specifier == 'subsec' || specifier == 'subsecond') {
      flags.subsecond = true;
    } else {
      return null;
    }

    const epoch = convertToDate(value, flags)?.getTime();
    if (epoch == null || !Number.isFinite(epoch)) {
      return null;
    }
    if (flags.subsecond) {
      return epoch / 1000.0;
    } else {
      return BigInt(Math.floor(epoch / 1000.0));
    }
  },
  parameters: [
    { name: 'value', type: ExpressionType.ANY, optional: false },
    { name: 'specifier', type: ExpressionType.ANY, optional: true },
    { name: 'specifier2', type: ExpressionType.ANY, optional: true }
  ],
  getReturnType(args) {
    return ExpressionType.INTEGER.or(ExpressionType.REAL);
  },
  detail: 'Convert a date to unix epoch'
};

const datetime: DocumentedSqlFunction = {
  debugName: 'datetime',
  call(value?: SqliteValue, specifier?: SqliteValue, specifier2?: SqliteValue) {
    if (value == null) {
      return null;
    }
    let flags: ParseDateFlags = {
      unixepoch: false,
      subsecond: false
    };
    if (specifier == null) {
    } else if (specifier == 'unixepoch') {
      flags.unixepoch = true;
      if (specifier2 == null) {
      } else if (specifier2 == 'subsec' || specifier2 == 'subsecond') {
        flags.subsecond = true;
      } else {
        return null;
      }
    } else if (specifier == 'subsec' || specifier == 'subsecond') {
      flags.subsecond = true;
    } else {
      return null;
    }

    const epoch = convertToDate(value, flags);
    if (epoch == null || !Number.isFinite(epoch.getTime())) {
      return null;
    }
    const baseString = epoch.toISOString().replace(/T/, ' ').replace(/^\-0+/, '-');
    if (flags.subsecond) {
      return baseString.replace(/Z$/, '');
    } else {
      return baseString.replace(/(\.\d+)?Z$/, '');
    }
  },
  parameters: [
    { name: 'value', type: ExpressionType.ANY, optional: false },
    { name: 'specifier', type: ExpressionType.ANY, optional: true },
    { name: 'specifier2', type: ExpressionType.ANY, optional: true }
  ],
  getReturnType(args) {
    return ExpressionType.TEXT;
  },
  detail: 'Convert a date string or unix epoch to a consistent date string'
};

const st_asgeojson: DocumentedSqlFunction = {
  debugName: 'st_asgeojson',
  call(geometry?: SqliteValue) {
    const geo = parseGeometry(geometry);
    if (geo == null) {
      return null;
    }
    return JSONBig.stringify(geo.toGeoJSON());
  },
  parameters: [{ name: 'geometry', type: ExpressionType.ANY, optional: false }],
  getReturnType(args) {
    return ExpressionType.TEXT;
  },
  detail: 'Covert PostGIS geometry to GeoJSON text'
};

const st_astext: DocumentedSqlFunction = {
  debugName: 'st_astext',
  call(geometry?: SqliteValue) {
    const geo = parseGeometry(geometry);
    if (geo == null) {
      return null;
    }
    return geo.toWkt();
  },
  parameters: [{ name: 'geometry', type: ExpressionType.ANY, optional: false }],
  getReturnType(args) {
    return ExpressionType.TEXT;
  },
  detail: 'Covert PostGIS geometry to WKT text'
};

const st_x: DocumentedSqlFunction = {
  debugName: 'st_x',
  call(geometry?: SqliteValue) {
    const geo = parseGeometry(geometry);
    if (geo == null) {
      return null;
    }
    if (geo instanceof wkx.Point) {
      return geo.x;
    }
    return null;
  },
  parameters: [{ name: 'geometry', type: ExpressionType.ANY, optional: false }],
  getReturnType(args) {
    return ExpressionType.REAL;
  },
  detail: 'Get the X value of a PostGIS point'
};

const st_y: DocumentedSqlFunction = {
  debugName: 'st_y',
  call(geometry?: SqliteValue) {
    const geo = parseGeometry(geometry);
    if (geo == null) {
      return null;
    }
    if (geo instanceof wkx.Point) {
      return geo.y;
    }
    return null;
  },
  parameters: [{ name: 'geometry', type: ExpressionType.ANY, optional: false }],
  getReturnType(args) {
    return ExpressionType.REAL;
  },
  detail: 'Get the Y value of a PostGIS point'
};

export const SQL_FUNCTIONS_NAMED = {
  upper,
  lower,
  substring,
  hex,
  length,
  base64,
  uuid_blob,
  typeof: fn_typeof,
  ifnull,
  iif,
  json_extract,
  json_array_length,
  json_valid,
  json_keys,
  unixepoch,
  datetime,
  st_asgeojson,
  st_astext,
  st_x,
  st_y
};

type FunctionName = keyof typeof SQL_FUNCTIONS_NAMED;

export const SQL_FUNCTIONS_CALL = Object.fromEntries(
  Object.entries(SQL_FUNCTIONS_NAMED).map(([name, fn]) => [name, fn.call])
) as Record<FunctionName, SqlFunction['call']>;

export const SQL_FUNCTIONS: Record<string, DocumentedSqlFunction> = SQL_FUNCTIONS_NAMED;

export const CAST_TYPES = new Set<String>(['text', 'numeric', 'integer', 'real', 'blob']);

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

export function castAsText(value: SqliteValue): string | null {
  if (value == null) {
    return null;
  } else if (value instanceof Uint8Array) {
    return textDecoder.decode(value);
  } else {
    return value.toString();
  }
}

export function castAsBlob(value: SqliteValue): Uint8Array | null {
  if (value == null) {
    return null;
  } else if (value instanceof Uint8Array) {
    return value!;
  }

  if (typeof value != 'string') {
    value = value.toString();
  }
  return textEncoder.encode(value);
}

export function cast(value: SqliteValue, to: string) {
  if (value == null) {
    return null;
  }
  if (to == 'text') {
    return castAsText(value);
  } else if (to == 'numeric') {
    if (value instanceof Uint8Array) {
      value = textDecoder.decode(value);
    }
    if (typeof value == 'string') {
      return parseNumeric(value);
    } else if (typeof value == 'number' || typeof value == 'bigint') {
      return value;
    } else {
      return 0n;
    }
  } else if (to == 'real') {
    if (value instanceof Uint8Array) {
      value = textDecoder.decode(value);
    }
    if (typeof value == 'string') {
      const nr = parseFloat(value);
      if (isNaN(nr)) {
        return 0.0;
      } else {
        return nr;
      }
    } else if (typeof value == 'number') {
      return value;
    } else if (typeof value == 'bigint') {
      return Number(value);
    } else {
      return 0.0;
    }
  } else if (to == 'integer') {
    if (value instanceof Uint8Array) {
      value = textDecoder.decode(value);
    }
    if (typeof value == 'string') {
      return parseBigInt(value);
    } else if (typeof value == 'number') {
      return Number.isInteger(value) ? BigInt(value) : BigInt(Math.floor(value));
    } else if (typeof value == 'bigint') {
      return value;
    } else {
      return 0n;
    }
  } else if (to == 'blob') {
    return castAsBlob(value);
  } else {
    throw new Error(`Type not supported for cast: '${to}'`);
  }
}

export function sqliteTypeOf(arg: SqliteValue) {
  if (arg == null) {
    return 'null';
  } else if (typeof arg == 'string') {
    return 'text';
  } else if (typeof arg == 'bigint') {
    return 'integer';
  } else if (typeof arg == 'number') {
    return 'real';
  } else if (arg instanceof Uint8Array) {
    return 'blob';
  } else {
    // Should not happen
    throw new Error(`Unknown type: ${arg}`);
  }
}

export function parseGeometry(value?: SqliteValue) {
  let blob: Buffer;
  if (value == null) {
    return null;
  } else if (value instanceof Uint8Array) {
    blob = Buffer.from(value);
  } else if (typeof value == 'string') {
    blob = Buffer.from(value, 'hex');
  } else {
    return null;
  }

  const geo = wkx.Geometry.parse(blob);
  return geo;
}

function parseNumeric(text: string): bigint | number {
  const match = /^\s*(\d+)(\.\d*)?(e[+\-]?\d+)?/i.exec(text);
  if (!match) {
    return 0n;
  }

  if (match[2] != null || match[3] != null) {
    const v = parseFloat(match[0]);
    return isNaN(v) ? 0n : v;
  } else {
    return BigInt(match[1]);
  }
}

function parseBigInt(text: string): bigint {
  const match = /^\s*(\d+)/.exec(text);
  if (!match) {
    return 0n;
  }
  return BigInt(match[1]);
}

function isNumeric(a: SqliteValue): a is number | bigint {
  return typeof a == 'number' || typeof a == 'bigint';
}

export function evaluateOperator(op: string, a: SqliteValue, b: SqliteValue): SqliteValue {
  switch (op) {
    case '=':
    case '!=':
    case '>':
    case '<':
    case '>=':
    case '<=': {
      if (a == null || b == null) {
        return null;
      }
      const diff = compare(a, b);
      if (op == '=') {
        return sqliteBool(diff === 0);
      } else if (op == '!=') {
        return sqliteBool(diff !== 0);
      } else if (op == '>') {
        return sqliteBool(diff > 0);
      } else if (op == '<') {
        return sqliteBool(diff < 0);
      } else if (op == '>=') {
        return sqliteBool(diff >= 0);
      } else if (op == '<=') {
        return sqliteBool(diff <= 0);
      } else {
        throw new Error('unreachable');
      }
    }
    // Not currently supported by the parser, but used with IS NULL
    case 'IS': {
      const diff = compare(a, b);
      return sqliteBool(diff === 0);
    }
    // Not currently supported by the parser, but used with IS NOT NULL
    case 'IS NOT': {
      const diff = compare(a, b);
      return sqliteBool(diff !== 0);
    }
    case '+':
    case '-':
    case '*':
    case '/':
      return doMath(op, a, b);
    case '||':
      return concat(a, b);
    case 'AND':
      return sqliteBool(sqliteBool(a) && sqliteBool(b));
    case 'OR':
      return sqliteBool(sqliteBool(a) || sqliteBool(b));
    case 'IN':
      if (a == null || b == null) {
        return null;
      }
      if (typeof b != 'string') {
        throw new Error('IN is only supported on JSON arrays');
      }
      const bParsed = JSON.parse(b);
      if (!Array.isArray(bParsed)) {
        throw new Error('IN is only supported on JSON arrays');
      }
      return sqliteBool(bParsed.includes(a));
    default:
      throw new Error(`Operator not supported: ${op}`);
  }
}

export function getOperatorReturnType(op: string, left: ExpressionType, right: ExpressionType) {
  switch (op) {
    case '=':
    case '!=':
    case '>':
    case '<':
    case '>=':
    case '<=': {
      return ExpressionType.INTEGER;
    }
    // Not currently supported by the parser, but used with IS NULL
    case 'IS': {
      return ExpressionType.INTEGER;
    }
    // Not currently supported by the parser, but used with IS NOT NULL
    case 'IS NOT': {
      return ExpressionType.INTEGER;
    }
    case '+':
    case '-':
    case '*':
    case '/':
      if (left.typeFlags == TYPE_INTEGER && right.typeFlags == TYPE_INTEGER) {
        // INT, INT stays INT
        return ExpressionType.INTEGER;
      } else if (left.isNumericOnly() && right.isNumericOnly()) {
        // INT, REAL or REAL, INT or REAL, REAL => always REAL
        return ExpressionType.REAL;
      } else {
        // Unknown - could be REAL or INT
        return ExpressionType.NUMERIC;
      }
    case '||':
      return ExpressionType.TEXT;
    case 'AND':
      return ExpressionType.INTEGER;
    case 'OR':
      return ExpressionType.INTEGER;
    case 'IN':
      return ExpressionType.INTEGER;
    default:
      return ExpressionType.NONE;
  }
}

function doMath(op: string, a: SqliteValue, b: SqliteValue) {
  if (a == null || b == null) {
    return null;
  }
  let na = cast(a, 'numeric') as number | bigint;
  let nb = cast(b, 'numeric') as number | bigint;

  if (typeof na == 'bigint' && typeof nb != 'bigint') {
    // bigint, real
    na = Number(na);
  } else if (typeof na != 'bigint' && typeof nb == 'bigint') {
    // real, bigint
    nb = Number(nb);
  }

  switch (op) {
    case '+':
      return (na as any) + (nb as any);
    case '-':
      return (na as any) - (nb as any);
    case '*':
      return (na as any) * (nb as any);
    case '/':
      return (na as any) / (nb as any);
    default:
      throw new Error(`Operator not supported: ${op}`);
  }
}

function concat(a: SqliteValue, b: SqliteValue): string | null {
  const aText = castAsText(a);
  const bText = castAsText(b);
  if (aText == null || bText == null) {
    return null;
  }
  return aText + bText;
}

export function jsonExtract(sourceValue: SqliteValue, path: SqliteValue, operator: string) {
  const valueText = castAsText(sourceValue);
  const pathText = castAsText(path);
  if (valueText == null || pathText == null) {
    return null;
  }

  const components = pathText.split('.');
  if (components[0] == '$') {
    components.shift();
  } else if (operator == 'json_extract') {
    throw new Error(`JSON path must start with $.`);
  }

  let value = JSONBig.parse(valueText) as any;
  for (let c of components) {
    if (value == null) {
      break;
    }
    value = value[c];
  }
  if (operator == '->') {
    // -> must always stringify, except when it's null
    if (value == null) {
      return null;
    }
    return JSONBig.stringify(value);
  } else {
    // Plain scalar value - simple conversion.
    return jsonValueToSqlite(value as string | number | bigint | boolean | null);
  }
}

export const OPERATOR_JSON_EXTRACT_JSON: SqlFunction = {
  debugName: 'operator->',
  call(json: SqliteValue, path: SqliteValue) {
    return jsonExtract(json, path, '->');
  },
  getReturnType(args) {
    return ExpressionType.ANY_JSON;
  }
};

export const OPERATOR_JSON_EXTRACT_SQL: SqlFunction = {
  debugName: 'operator->>',
  call(json: SqliteValue, path: SqliteValue) {
    return jsonExtract(json, path, '->>');
  },
  getReturnType(_args) {
    return ExpressionType.ANY_JSON;
  }
};

export const OPERATOR_IS_NULL: SqlFunction = {
  debugName: 'operator_is_null',
  call(value: SqliteValue) {
    return evaluateOperator('IS', value, null);
  },
  getReturnType(_args) {
    return ExpressionType.INTEGER;
  }
};

export const OPERATOR_IS_NOT_NULL: SqlFunction = {
  debugName: 'operator_is_not_null',
  call(value: SqliteValue) {
    return evaluateOperator('IS NOT', value, null);
  },
  getReturnType(_args) {
    return ExpressionType.INTEGER;
  }
};

export const OPERATOR_NOT: SqlFunction = {
  debugName: 'operator_not',
  call(value: SqliteValue) {
    return sqliteNot(value);
  },
  getReturnType(_args) {
    return ExpressionType.INTEGER;
  }
};

export const OPERATOR_IN = getOperatorFunction('IN');

export function castOperator(castTo: string | undefined): SqlFunction | null {
  if (castTo == null || !CAST_TYPES.has(castTo)) {
    return null;
  }
  return {
    debugName: `operator_cast_${castTo}`,
    call(value: SqliteValue) {
      if (value == null) {
        return null;
      }
      return cast(value, castTo!);
    },
    getReturnType(_args) {
      return ExpressionType.fromTypeText(castTo as SqliteType);
    }
  };
}

export interface ParseDateFlags {
  /**
   * True if input is unixepoch instead of julien days
   */
  unixepoch?: boolean;
  /**
   * True if output should include milliseconds
   */
  subsecond?: boolean;
}

export function convertToDate(dateTime: SqliteValue, flags: ParseDateFlags): Date | null {
  if (typeof dateTime == 'string') {
    return parseUTCDate(dateTime);
  } else if (typeof dateTime == 'bigint') {
    if (flags.unixepoch) {
      return new Date(Number(dateTime) * 1000.0);
    } else {
      return julianToJSDate(Number(dateTime));
    }
  } else if (typeof dateTime == 'number') {
    if (flags.unixepoch) {
      return new Date(dateTime * 1000.0);
    } else {
      return julianToJSDate(dateTime);
    }
  } else {
    return null;
  }
}

function parseUTCDate(isoDateString: string) {
  const hasTimezone = /[Zz]|[+\-]\d{2}:\d{2}$/;
  const isJulienDay = /^\d+(\.\d*)?$/;
  if (hasTimezone.test(isoDateString)) {
    // If the string already has a timezone, parse it directly
    return new Date(isoDateString);
  } else if (isJulienDay.test(isoDateString)) {
    return julianToJSDate(parseFloat(isoDateString));
  } else {
    // If the string has no timezone, append "Z" to the end to interpret it as UTC
    return new Date(isoDateString + 'Z');
  }
}

function julianToJSDate(julianDay: number) {
  // The Julian date for the Unix Epoch is 2440587.5
  const julianAtEpoch = 2440587.5;

  // Calculate the difference between the Julian date and the Unix Epoch in days
  const daysSinceEpoch = julianDay - julianAtEpoch;

  // Convert this to milliseconds
  const msSinceEpoch = daysSinceEpoch * 24 * 60 * 60 * 1000;

  // Create a new Date object with this number of milliseconds since the Unix Epoch
  return new Date(msSinceEpoch);
}

const TYPE_ORDERING = {
  null: 0,
  integer: 1,
  real: 1,
  text: 2,
  blob: 3
};

function compare(a: SqliteValue, b: SqliteValue): number {
  // https://www.sqlite.org/datatype3.html#comparisons
  if (a == null && b == null) {
    // Only for IS / IS NOT
    return 0;
  }
  if ((isNumeric(a) && isNumeric(b)) || (typeof a == 'string' && typeof b == 'string')) {
    if (a == b) {
      return 0;
    } else if (a > b) {
      return 1;
    } else {
      return -1;
    }
  } else if (a instanceof Uint8Array && b instanceof Uint8Array) {
    throw new Error('Comparing blobs is not supported currently');
  }

  const typeA = sqliteTypeOf(a);
  const typeB = sqliteTypeOf(b);

  return TYPE_ORDERING[typeA] - TYPE_ORDERING[typeB];
}
