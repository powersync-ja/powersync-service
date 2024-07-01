import * as schema_validator from '../validators/schema-validator.js';
import * as defs from '../definitions.js';

/**
 * Recursively walk a given schema resolving a list of refs that are actively used in some way by the
 * root schema. This information can then later be used to prune unused definitions.
 *
 * This only works for top-level references to 'definitions' as this is intended to be used in
 * conjunction with tools that generate schemas in deterministic ways. For a more general
 * implementation one should make use of `$RefParser`.
 *
 * We don't use this here as it resolves to a Promise, which we want to avoid for this tool
 */
export const findUsedRefs = (schema: any, definitions = schema.definitions, cache: string[] = []): string[] => {
  if (Array.isArray(schema)) {
    return schema
      .map((subschema) => {
        return findUsedRefs(subschema, definitions, cache);
      })
      .flat();
  }

  if (typeof schema === 'object') {
    return Object.keys(schema).reduce((used: string[], key) => {
      const value = schema[key];
      if (key === '$ref') {
        if (cache.includes(value)) {
          return used;
        }
        cache.push(value);
        const subschema = definitions[value.replace('#/definitions/', '')];
        used.push(value, ...findUsedRefs(subschema, definitions, cache));
        return used;
      }
      if (key === 'definitions') {
        return used;
      }
      return used.concat(findUsedRefs(value, definitions, cache));
    }, []);
  }

  return [];
};

/**
 * Prune a given subschema definitions map by comparing keys against a given collection of
 * definition keys that are referenced in some way, either directly or indirectly, by the
 * root schema
 */
export const pruneDefinitions = (definitions: Record<string, any>, refs: string[]) => {
  return Object.keys(definitions).reduce((pruned: Record<string, any>, key) => {
    if (refs.includes(`#/definitions/${key}`)) {
      pruned[key] = definitions[key];
    }
    return pruned;
  }, {});
};

export type CompilerFunction = () => defs.JSONSchema;
export type ValidatorFunction = <T>(
  params?: schema_validator.CreateSchemaValidatorParams
) => schema_validator.SchemaValidator<T>;

export type Compilers = {
  compile: CompilerFunction;
  validator: ValidatorFunction;
};
export type WithCompilers<T> = Compilers & {
  [P in keyof T]: T[P] extends Record<string, any> ? WithCompilers<T[P]> : T[P];
};

/**
 * Given a JSON Schema containing a `definitions` entry, return a Proxy representation of the same
 * schema which responds to `compile` and `validator` arbitrarily deep.
 *
 * Calling compile on a sub-schema will 'inject' the root schema `definitions` mapping and remove
 * the Proxy wrapping.
 *
 * Calling `validator` on a sub-schema will `compile` and then create a SchemaValidator from the
 * resulting schema
 */
export const parseJSONSchema = <T extends defs.JSONSchema>(
  schema: T,
  definitions = schema.definitions
): WithCompilers<T> => {
  return new Proxy(schema, {
    get(target: any, prop) {
      const compile: CompilerFunction = () => {
        const schema = {
          definitions: definitions,
          ...target
        };

        if (!schema.definitions) {
          return schema;
        }

        const used = findUsedRefs(schema);
        return {
          ...schema,
          definitions: pruneDefinitions(schema.definitions, used)
        };
      };
      const validator: ValidatorFunction = (options) => {
        return schema_validator.createSchemaValidator(compile(), options);
      };

      if (prop === 'compile') {
        return compile;
      }
      if (prop === 'validator') {
        return validator;
      }

      const subschema = target[prop];

      if (Array.isArray(subschema)) {
        return subschema;
      }
      if (typeof subschema !== 'object') {
        return subschema;
      }

      return parseJSONSchema(subschema, definitions);
    }
  });
};
