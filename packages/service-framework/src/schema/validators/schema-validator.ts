// @ts-ignore
import AjvErrorFormatter from 'better-ajv-errors';
import AJV from 'ajv';

import * as defs from '../definitions.js';
import * as utils from '../utils.js';
import * as keywords from '../json-schema/keywords.js';

export class SchemaValidatorError extends Error {
  constructor(message: string) {
    super(message);

    this.name = this.constructor.name;
  }
}

export type SchemaValidator<T> = defs.MicroValidator<T>;

export type CreateSchemaValidatorParams = {
  ajv?: AJV.Options;

  /**
   * Allow making the given schema loosely typed to allow accepting additional properties. This
   * is useful in certain scenarios such as accepting kafka events that are going through a
   * migration and having additional properties set
   */
  allowAdditional?: boolean;

  fail_fast?: boolean;
};

/**
 * Create a validator from a given JSON-Schema schema object. This makes uses of AJV internally
 * to compile a validation function
 */
export const createSchemaValidator = <T = any>(
  schema: defs.JSONSchema,
  params: CreateSchemaValidatorParams = {}
): SchemaValidator<T> => {
  try {
    const ajv = new AJV.Ajv({
      allErrors: !(params.fail_fast ?? false),
      keywords: [keywords.BufferNodeType],
      ...(params.ajv || {})
    });

    let processed_schema = schema;
    if (params.allowAdditional) {
      processed_schema = utils.allowAdditionalProperties(processed_schema);
    }

    const validator = ajv.compile(processed_schema);

    return {
      toJSONSchema: () => {
        return schema;
      },

      validate: (data) => {
        const valid = validator(data);

        if (!valid) {
          const errors = AjvErrorFormatter(processed_schema, data, validator.errors || [], {
            format: 'js'
          })?.map((error: any) => error.error);

          return {
            valid: false,
            errors: errors || []
          };
        }

        return {
          valid: true
        };
      }
    };
  } catch (err) {
    // Here we re-throw the error because the original error thrown by AJV has a deep stack that
    // obfuscates the location of the error in application code
    throw new SchemaValidatorError(err.message);
  }
};
