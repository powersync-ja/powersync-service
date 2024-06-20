import * as errors from '../errors/errors-index.js';
import * as defs from './definitions.js';

export type Schema = {
  additionalProperties?: boolean | Schema;
  [key: string]: any;
};

/**
 * Utility function to strip out `additionalProperties` fields from a given JSON-Schema. This can be used
 * to make a schema less strict which may be necessary for certain use-cases
 */
export const allowAdditionalProperties = <T extends Schema>(schema: T): T => {
  return Object.keys(schema).reduce((next_schema: any, key) => {
    const value = schema[key];

    if (key === 'additionalProperties' && typeof value === 'boolean') {
      return next_schema;
    }

    if (Array.isArray(value)) {
      next_schema[key] = value.map((value) => {
        if (typeof value !== 'object') {
          return value;
        }
        return allowAdditionalProperties(value);
      });
    } else if (typeof value === 'object') {
      next_schema[key] = allowAdditionalProperties(value);
    } else {
      next_schema[key] = value;
    }

    return next_schema;
  }, {});
};

/**
 * A small utility for validating some data using a MicroValidator. Will return the valid data (typed correctly) or throw
 * a validation error
 */
export const validateData = <T>(event: any, validator: defs.MicroValidator<T>): T => {
  const result = validator.validate(event);
  if (!result.valid) {
    throw new errors.ValidationError(result.errors);
  }
  return event;
};
