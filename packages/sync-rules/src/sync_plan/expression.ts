/**
 * An enumeration of all scalar SQL expressions supported by the sync service.
 *
 * Note that these expressions can be serialized, and the evaluation of serialized expressions must be stable.
 *
 * The `Data` type parameter encodes what external data references an {@link ExternalData} node may reference: Bucket
 * data sources can only reference row data (e.g. to evaluate filters), while queriers can only reference connection
 * data. Adding the parameter to expressions ensures the entire subtree can only reference expected sources.
 */
export type SqlExpression<Data> =
  | ExternalData<Data>
  | UnaryExpression<Data>
  | BinaryExpression<Data>
  | BetweenExpression<Data>
  | ScalarInExpression<Data>
  | CaseWhenExpression<Data>
  | CastExpression<Data>
  | ScalarFunctionCallExpression<Data>
  | LiteralExpression;

/**
 * External data injected into the expression during evaluation.
 *
 * What kinds of external data is allowed depends on the expression. For instance, expressions for bucket data sources
 * may only reference columns in the current row.
 */
export type ExternalData<Data> = { type: 'data'; source: Data };

export type UnaryOperator = 'not' | '+'; //| '-' | '~';

export type UnaryExpression<Data> = {
  type: 'unary';
  operand: SqlExpression<Data>;
  operator: UnaryOperator;
};

export type BinaryOperator =
  | 'or'
  | 'and'
  | '='
  | 'is'
  | '<'
  | '<='
  | '>'
  | '>='
  | '&'
  | '|'
  | '<<'
  | '>>'
  | '+'
  | '-'
  | '*'
  | '/'
  | '%'
  | '||';

/**
 * A binary expression in SQLite, `$left $op $right`.
 *
 * Note that the `LIKE`, `GLOB`, `REGEXP` and `MATCH`, `->` and `->>` operators are not represented as binary
 * expressions but rather as the functions SQLite [would call for them](https://www.sqlite.org/lang_expr.html#the_like_glob_regexp_match_and_extract_operators).
 * Also, note that negated operators (e.g. `!=` or `NOT IN`) are represented by wrapping the expression in a unary
 * `NOT`. This makes transformations to DNF easier.
 */
export type BinaryExpression<Data> = {
  type: 'binary';
  left: SqlExpression<Data>;
  operator: BinaryOperator;
  right: SqlExpression<Data>;
};

export type BetweenExpression<Data> = {
  type: 'between';
  value: SqlExpression<Data>;
  low: SqlExpression<Data>;
  high: SqlExpression<Data>;
};

/**
 * An `expr IN ($in0, $in1, ..., $inN)` expression.
 */
export type ScalarInExpression<Data> = {
  type: 'scalar_in';
  target: SqlExpression<Data>;
  in: SqlExpression<Data>[];
};

/**
 * A `CASE WHEN` expression as supported by SQLite.
 */
export type CaseWhenExpression<Data> = {
  type: 'case_when';
  operand?: SqlExpression<Data>;
  whens: { when: SqlExpression<Data>; then: SqlExpression<Data> }[];
  else?: SqlExpression<Data>;
};

/**
 * A `CAST` expression.
 */
export type CastExpression<Data> = {
  type: 'cast';
  operand: SqlExpression<Data>;
  cast_as: 'text' | 'numeric' | 'real' | 'integer' | 'blob';
};

/**
 * A constant literal in SQL.
 */
export type LiteralExpression =
  | { type: 'lit_null' }
  | { type: 'lit_double'; value: number }
  | {
      type: 'lit_int';
      /**
       * The integer value as a base 10 string. We don't use the correct `bigint` type to be able to serialize and
       * deserialize with the default JSON implementation.
       */
      base10: string;
    }
  | { type: 'lit_string'; value: string };

/**
 * A scalar (non-aggregate, non-window, non-table-valued) function call in SQL.
 */
export type ScalarFunctionCallExpression<Data> = {
  type: 'function';
  function: string;
  parameters: SqlExpression<Data>[];
};

export type ArgumentCount = number | { min: number; max?: number; mustBeEven?: boolean; mustBeOdd?: boolean };

export const supportedFunctions: Record<string, ArgumentCount> = {
  // https://sqlite.org/lang_corefunc.html#list_of_core_functions
  abs: 1,
  char: { min: 0 },
  coalesce: { min: 2 },
  concat: { min: 1 },
  concat_ws: { min: 2 },
  format: { min: 1 },
  glob: 2,
  hex: 1,
  ifnull: { min: 2 },
  if: { min: 2 },
  iif: { min: 2 },
  instr: 2,
  length: 1,
  // TODO: Establish defaults for case sensitivity, changing escape characters, ICU support.
  // We might just want to remove LIKE support since we don't seem to have it in sql_functions.ts
  like: { min: 2, max: 3 },
  //  likelihood: 2,
  //  likely: 1,
  lower: 1,
  ltrim: { min: 1, max: 2 },
  max: { min: 2 },
  min: { min: 2 },
  nullif: 2,
  octet_length: 1,
  printf: { min: 1 },
  quote: 1,
  replace: 3,
  round: { min: 1, max: 2 },
  rtrim: { min: 1, max: 2 },
  sign: 1,
  substr: { min: 2, max: 3 },
  substring: { min: 2, max: 3 },
  trim: { min: 1, max: 2 },
  typeof: 1,
  unhex: { min: 1, max: 2 },
  unicode: 1,
  unistr: 1,
  unistr_quote: 1,
  //  unlikely: 1,
  upper: 1,
  zeroblob: 1,
  // Scalar functions from https://sqlite.org/json1.html#overview
  // We disallow jsonb functions because they're not implemented in sql_functions.ts and we want to preserve
  // compatibility with that.
  json: 1,
  //  jsonb: 1,
  json_array: { min: 0 },
  //  jsonb_array: { min: 0 },
  json_array_length: { min: 1, max: 2 },
  json_error_position: 1,
  json_extract: { min: 2 },
  //  jsonb_extract: { min: 2 },
  json_insert: { min: 3, mustBeOdd: true },
  //  jsonb_insert: { min: 3, mustBeOdd: true },
  json_object: { min: 0, mustBeEven: true },
  //  jsonb_object: { min: 0, mustBeEven: true },
  json_patch: 2,
  //  jsonb_patch: 2,
  json_pretty: 1,
  json_remove: { min: 2 },
  //  jsonb_remove: { min: 2 },
  json_replace: { min: 3, mustBeOdd: true },
  //  jsonb_replace: { min: 3, mustBeOdd: true },
  json_set: { min: 3, mustBeOdd: true },
  //  jsonb_set: { min: 3, mustBeOdd: true },
  json_type: { min: 1, max: 2 },
  json_valid: { min: 1, max: 2 },
  json_quote: { min: 1 },

  // https://www.sqlite.org/lang_datefunc.html, but we only support datetime and unixepoch
  // TODO: Ensure our support matches the current unixepoch behavior in sql_functions.ts, register as user-defined
  // function to patch default if in doubt.
  unixepoch: { min: 1 },
  datetime: { min: 1 },

  // PowerSync-specific, mentioned in https://docs.powersync.com/sync/rules/supported-sql#functions
  base64: 1,
  json_keys: 1,
  uuid_blob: 1,
  st_asgeojson: 1,
  st_astext: 1,
  st_x: 1,
  st_y: 1
};
