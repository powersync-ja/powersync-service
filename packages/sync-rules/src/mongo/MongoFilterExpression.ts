import { z } from 'zod';
import { YamlError } from '../errors.js';
import type { JsonObject } from '../json.js';
import type { SyncConfigSourceLocationResolver, SyncConfigSourcePath } from '../SyncConfigDiagnostics.js';
import { MONGO_BSON_VALIDATORS } from './MongoBsonValidators.js';
import { MongoFilterValidationError } from './MongoFilterValidation.js';

/**
 * Reuses BSON checks directly, without converting their errors through AJV keywords.
 */
function bson<T extends z.ZodType>(schema: T, name: keyof typeof MONGO_BSON_VALIDATORS) {
  return schema.superRefine((value, context) => {
    try {
      MONGO_BSON_VALIDATORS[name].validate(value);
    } catch (error) {
      if (!(error instanceof MongoFilterValidationError)) throw error;
      context.addIssue({ code: 'custom', message: error.detail, path: [...error.path] });
    }
  });
}

const INTEGER = z.string().regex(/^-?(?:0|[1-9][0-9]*)$/);
const FIELD = z
  .string({ error: 'Expected a source field such as $$doc.name.' })
  .regex(/^\$\$doc(?:\.(?![0-9]+(?:\.|$))[^.$][^.]*)+$/, 'Expected a source field such as $$doc.name.');
const NUMBER = bson(z.number(), 'mongoNumber');
const STRING = z.string().regex(/^(?!\$)/, 'Use $literal for text starting with $.');
const DATE_STRING = bson(
  z.string().regex(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/),
  'mongoDateString'
);
// Extended JSON wrappers retain BSON types that plain JSON values cannot express.
const WRAPPERS = {
  $literal: z.string(),
  $oid: z.string().regex(/^[0-9a-fA-F]{24}$/),
  $numberInt: bson(INTEGER, 'mongoInt32'),
  $numberLong: bson(INTEGER, 'mongoInt64'),
  $numberDouble: bson(z.string().regex(/^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?$/), 'mongoDouble'),
  $numberDecimal: bson(z.string(), 'mongoDecimal128'),
  $uuid: z.string().regex(/^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/),
  $date: z.union([DATE_STRING, z.strictObject({ $numberLong: bson(INTEGER, 'mongoDateMillis') })]),
  $timestamp: z.strictObject({
    t: z.number().int().min(0).max(0xffffffff),
    i: z.number().int().min(0).max(0xffffffff)
  }),
  $binary: bson(
    z.strictObject({
      base64: z.string().regex(/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/][AQgw]==|[A-Za-z0-9+/]{2}[AEIMQUYcgkosw048]=)?$/),
      subType: z.string().regex(/^[0-9a-fA-F]{2}$/)
    }),
    'mongoBinary'
  )
};
/**
 * MongoDB comparisons take a field reference and a scalar or list operand.
 */
function comparison<T extends z.ZodType>(value: T) {
  return z.tuple([FIELD, value]);
}

/**
 * Defines operand shapes once for the structural schema and runtime validator.
 * With dispatch=false, explicit unions describe every allowed shape for JSON Schema generation and autocomplete.
 * With dispatch=true, validation selects the schema for the supplied operator or literal type, avoiding union
 * errors from unrelated alternatives. This custom selection cannot be represented in generated JSON Schema,
 * so both schemas share the operand definitions but use different selection logic.
 */
function createGrammar({ dispatch }: { dispatch: boolean }): z.ZodType<MongoFilterExpression> {
  const wrappers = keyed(WRAPPERS, dispatch);
  const literal = dispatch
    ? z.unknown().superRefine((value, context) => {
        const schema =
          typeof value === 'string'
            ? STRING
            : typeof value === 'number'
              ? NUMBER
              : typeof value === 'boolean'
                ? z.boolean()
                : wrappers;
        forwardIssues(schema, value, context);
      })
    : z.union([z.boolean(), NUMBER, STRING, wrappers]);
  // Defer resolution so boolean operators can contain nested expressions.
  const expression: z.ZodType<MongoFilterExpression> = z
    .lazy(() => keyed(operands, dispatch))
    .meta(dispatch ? {} : { id: 'mongodb_filter_expression' }) as z.ZodType<MongoFilterExpression>;
  const operands = {
    $eq: comparison(literal).meta({
      description: 'Compare a source field with a scalar constant.',
      examples: [['$$doc.status', 'active']]
    }),
    $in: comparison(z.array(literal).min(1)).meta({
      examples: [['$$doc.status', ['pending', 'shipped']]]
    }),
    $and: z
      .array(expression)
      .min(1)
      .meta({ examples: [[{ $eq: ['$$doc.active', true] }]] }),
    $or: z
      .array(expression)
      .min(1)
      .meta({ examples: [[{ $eq: ['$$doc.status', 'pending'] }, { $eq: ['$$doc.priority', true] }]] })
  };
  return expression;
}

/**
 * Chooses one named payload schema without trying unrelated alternatives.
 * Zod tuples and arrays handle operand validation, recursion, and issue paths.
 */
function keyed(schemas: Record<string, z.ZodType>, dispatch: boolean): z.ZodType {
  if (!dispatch) {
    return z.union(Object.entries(schemas).map(([key, schema]) => z.strictObject({ [key]: schema })));
  }
  return z.unknown().superRefine((value, context) => {
    if (value === null || typeof value !== 'object' || Array.isArray(value) || Object.keys(value).length !== 1) {
      context.addIssue({
        code: 'custom',
        message: `Expected exactly one supported ${Object.hasOwn(schemas, '$eq') ? 'filter operator' : 'literal wrapper'}.`
      });
      return;
    }
    const key = Object.keys(value)[0];
    if (!Object.hasOwn(schemas, key)) {
      context.addIssue({
        code: 'custom',
        path: [key],
        params: { target: 'key' },
        message: `Expected one of ${Object.keys(schemas).join(', ')}.`
      });
      return;
    }
    forwardIssues(schemas[key], (value as Record<string, unknown>)[key], context, [key]);
  });
}

/**
 * Adds selected-schema issues to the enclosing Zod context, keeping nested paths intact.
 */
function forwardIssues(schema: z.ZodType, value: unknown, context: z.RefinementCtx, prefix: PropertyKey[] = []) {
  const result = schema.safeParse(value);
  if (!result.success) {
    for (const issue of result.error.issues) {
      context.addIssue({ ...issue, path: [...prefix, ...issue.path] });
    }
  }
}

// Structural alternatives support editor autocomplete; runtime dispatch gives focused errors.
export const MONGO_FILTER_SCHEMA = createGrammar({ dispatch: false });
export const MONGO_FILTER_VALIDATOR = createGrammar({ dispatch: true });
// Export as a child so Zod names recursive references using its metadata ID instead of `#`.
// These definitions can then be embedded directly in the complete Sync Config schema.
export const MONGO_FILTER_JSON_SCHEMA_DEFINITIONS = z.toJSONSchema(
  z.object({ mongodb_filter_expression: MONGO_FILTER_SCHEMA }),
  {
    // Match the draft supported by our default AJV validator.
    target: 'draft-7',
    io: 'input',
    // Share repeated schemas instead of expanding them at every occurrence.
    reused: 'ref'
  }
).definitions as unknown as JsonObject;

/**
 * Validates authored input without replacing it with Zod's parsed output.
 *
 * The base path locates the expression within the config. Validation errors may point inside nested
 * expression objects or operand arrays, so their relative paths are appended to that base path and
 * resolved through sourceLocationResolver to highlight the specific key or value in the original source.
 */
export function parseMongoFilterExpression({
  value,
  basePath,
  sourceLocationResolver
}: {
  value: unknown;
  basePath: SyncConfigSourcePath;
  sourceLocationResolver: SyncConfigSourceLocationResolver;
}): YamlError[] {
  const result = MONGO_FILTER_VALIDATOR.safeParse(value);
  const errors: YamlError[] = [];
  for (const issue of result.success ? [] : result.error.issues) {
    // Extra properties point to their keys; operand failures point to their values.
    const targets =
      issue.code === 'unrecognized_keys'
        ? issue.keys.map((key) => ({ path: [...issue.path, key], target: 'key' as const }))
        : [
            {
              path: issue.path,
              target: issue.code === 'custom' && issue.params?.target === 'key' ? ('key' as const) : ('value' as const)
            }
          ];
    for (const { path: relativePath, target } of targets) {
      const path = [...basePath, ...relativePath.map((part) => (typeof part === 'symbol' ? String(part) : part))];
      errors.push(
        new YamlError(
          new Error(`Invalid MongoDB pre-filtering expression: ${issue.message}`),
          sourceLocationResolver.getLocation(path, target)
        )
      );
    }
  }
  return errors;
}

export type MongoFieldPath = `$$doc.${string}`;
export type MongoExpressionLiteral =
  | boolean
  | number
  | string
  | {
      [Key in keyof typeof WRAPPERS]: { [Property in Key]: z.output<(typeof WRAPPERS)[Key]> };
    }[keyof typeof WRAPPERS];
export type MongoFilterExpression =
  | { $eq: [MongoFieldPath, MongoExpressionLiteral] }
  | { $in: [MongoFieldPath, MongoExpressionLiteral[]] }
  | { $and: MongoFilterExpression[] }
  | { $or: MongoFilterExpression[] };
export type MongoTableFilter = MongoFilterExpression | 'disabled';
