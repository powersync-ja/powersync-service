import { LineCounter, parseDocument, Scalar, YAMLMap, YAMLSeq } from 'yaml';
import { SqlRuleError, SyncRulesErrors, YamlError } from './errors.js';
import { IdSequence } from './IdSequence.js';
import { validateSyncRulesSchema } from './json_schema.js';
import { SourceTableInterface } from './SourceTableInterface.js';
import { QueryParseResult, SqlBucketDescriptor } from './SqlBucketDescriptor.js';
import { TablePattern } from './TablePattern.js';
import {
  EvaluatedParameters,
  EvaluatedParametersResult,
  EvaluatedRow,
  EvaluateRowOptions,
  EvaluationError,
  EvaluationResult,
  isEvaluatedParameters,
  isEvaluatedRow,
  isEvaluationError,
  QueryBucketIdOptions,
  QueryParseOptions,
  RequestParameters,
  SourceSchema,
  SqliteRow,
  SyncRules
} from './types.js';

const ACCEPT_POTENTIALLY_DANGEROUS_QUERIES = Symbol('ACCEPT_POTENTIALLY_DANGEROUS_QUERIES');

export class SqlSyncRules implements SyncRules {
  bucket_descriptors: SqlBucketDescriptor[] = [];
  idSequence = new IdSequence();

  content: string;

  errors: YamlError[] = [];

  static validate(yaml: string, options?: { schema?: SourceSchema }): YamlError[] {
    try {
      const rules = this.fromYaml(yaml, options);
      return rules.errors;
    } catch (e) {
      if (e instanceof SyncRulesErrors) {
        return e.errors;
      } else if (e instanceof YamlError) {
        return [e];
      } else {
        return [new YamlError(e)];
      }
    }
  }

  static fromYaml(yaml: string, options?: { throwOnError?: boolean; schema?: SourceSchema }) {
    const throwOnError = options?.throwOnError ?? true;
    const schema = options?.schema;

    const lineCounter = new LineCounter();
    const parsed = parseDocument(yaml, {
      schema: 'core',
      keepSourceTokens: true,
      lineCounter,
      customTags: [
        {
          tag: '!accept_potentially_dangerous_queries',
          resolve(_text: string, _onError: (error: string) => void) {
            return ACCEPT_POTENTIALLY_DANGEROUS_QUERIES;
          }
        }
      ]
    });

    const rules = new SqlSyncRules(yaml);

    if (parsed.errors.length > 0) {
      rules.errors.push(
        ...parsed.errors.map((error) => {
          return new YamlError(error);
        })
      );

      if (throwOnError) {
        rules.throwOnError();
      }
      return rules;
    }

    const bucketMap = parsed.get('bucket_definitions') as YAMLMap;
    if (bucketMap == null) {
      rules.errors.push(new YamlError(new Error(`'bucket_definitions' is required`)));

      if (throwOnError) {
        rules.throwOnError();
      }
      return rules;
    }

    for (let entry of bucketMap.items) {
      const { key: keyScalar, value } = entry as { key: Scalar; value: YAMLMap };
      const key = keyScalar.toString();

      const accept_potentially_dangerous_queries =
        value.get('accept_potentially_dangerous_queries', true)?.value == true;
      const options: QueryParseOptions = {
        accept_potentially_dangerous_queries
      };
      const parameters = value.get('parameters', true) as unknown;
      const dataQueries = value.get('data', true) as unknown;

      const descriptor = new SqlBucketDescriptor(key, rules.idSequence);

      if (parameters instanceof Scalar) {
        rules.withScalar(parameters, (q) => {
          return descriptor.addParameterQuery(q, schema, options);
        });
      } else if (parameters instanceof YAMLSeq) {
        for (let item of parameters.items) {
          rules.withScalar(item, (q) => {
            return descriptor.addParameterQuery(q, schema, options);
          });
        }
      } else {
        descriptor.addParameterQuery('SELECT', schema, options);
      }

      if (!(dataQueries instanceof YAMLSeq)) {
        rules.errors.push(this.tokenError(dataQueries ?? value, `'data' must be an array`));
        continue;
      }
      for (let query of dataQueries.items) {
        rules.withScalar(query, (q) => {
          return descriptor.addDataQuery(q, schema);
        });
      }
      rules.bucket_descriptors.push(descriptor);
    }

    // Validate that there are no additional properties.
    // Since these errors don't contain line numbers, do this last.
    const valid = validateSyncRulesSchema(parsed.toJSON());
    if (!valid) {
      rules.errors.push(
        ...validateSyncRulesSchema.errors!.map((e: any) => {
          return new YamlError(e);
        })
      );
    }

    if (throwOnError) {
      rules.throwOnError();
    }

    return rules;
  }

  throwOnError() {
    if (this.errors.filter((e) => e.type != 'warning').length > 0) {
      throw new SyncRulesErrors(this.errors);
    }
  }

  static tokenError(token: any, message: string) {
    const start = token?.srcToken?.offset ?? 0;
    const end = start + 1;
    return new YamlError(new Error(message), { start, end });
  }

  withScalar(scalar: Scalar, cb: (value: string) => QueryParseResult) {
    const value = scalar.toString();

    const wrapped = (value: string): QueryParseResult => {
      try {
        return cb(value);
      } catch (e) {
        return {
          parsed: false,
          errors: [e]
        };
      }
    };

    const result = wrapped(value);
    for (let err of result.errors) {
      let sourceOffset = scalar.srcToken!.offset;
      if (scalar.type == Scalar.QUOTE_DOUBLE || scalar.type == Scalar.QUOTE_SINGLE) {
        // TODO: Is there a better way to do this?
        sourceOffset += 1;
      }
      let offset: number;
      let end: number;
      if (err instanceof SqlRuleError && err.location) {
        offset = err.location!.start + sourceOffset;
        end = err.location!.end + sourceOffset;
      } else if (typeof (err as any).token?._location?.start == 'number') {
        offset = sourceOffset + (err as any).token?._location?.start;
        end = sourceOffset + (err as any).token?._location?.end;
      } else {
        offset = sourceOffset;
        end = sourceOffset + Math.max(value.length, 1);
      }

      const pos = { start: offset, end };
      this.errors.push(new YamlError(err, pos));
    }
    return result;
  }

  constructor(content: string) {
    this.content = content;
  }

  /**
   * Throws errors.
   */
  evaluateRow(options: EvaluateRowOptions): EvaluatedRow[] {
    const { results, errors } = this.evaluateRowWithErrors(options);
    if (errors.length > 0) {
      throw new Error(errors[0].error);
    }
    return results;
  }

  evaluateRowWithErrors(options: EvaluateRowOptions): { results: EvaluatedRow[]; errors: EvaluationError[] } {
    let rawResults: EvaluationResult[] = [];
    for (let query of this.bucket_descriptors) {
      rawResults.push(...query.evaluateRow(options));
    }

    const results = rawResults.filter(isEvaluatedRow) as EvaluatedRow[];
    const errors = rawResults.filter(isEvaluationError) as EvaluationError[];

    return { results, errors };
  }

  /**
   * Throws errors.
   */
  evaluateParameterRow(table: SourceTableInterface, row: SqliteRow): EvaluatedParameters[] {
    const { results, errors } = this.evaluateParameterRowWithErrors(table, row);
    if (errors.length > 0) {
      throw new Error(errors[0].error);
    }
    return results;
  }

  evaluateParameterRowWithErrors(
    table: SourceTableInterface,
    row: SqliteRow
  ): { results: EvaluatedParameters[]; errors: EvaluationError[] } {
    let rawResults: EvaluatedParametersResult[] = [];
    for (let query of this.bucket_descriptors) {
      rawResults.push(...query.evaluateParameterRow(table, row));
    }

    const results = rawResults.filter(isEvaluatedParameters) as EvaluatedParameters[];
    const errors = rawResults.filter(isEvaluationError) as EvaluationError[];
    return { results, errors };
  }

  /**
   * @deprecated For testing only.
   */
  getStaticBucketIds(parameters: RequestParameters) {
    let results: string[] = [];
    for (let bucket of this.bucket_descriptors) {
      results.push(...bucket.getStaticBucketIds(parameters));
    }
    return results;
  }

  /**
   * Note: This can error hard.
   */
  async queryBucketIds(options: QueryBucketIdOptions): Promise<string[]> {
    let results: string[] = [];
    for (let bucket of this.bucket_descriptors) {
      results.push(...(await bucket.queryBucketIds(options)));
    }
    return results;
  }

  getSourceTables(): TablePattern[] {
    let sourceTables = new Map<String, TablePattern>();
    for (let bucket of this.bucket_descriptors) {
      for (let r of bucket.getSourceTables()) {
        const key = `${r.connectionTag}.${r.schema}.${r.tablePattern}`;
        sourceTables.set(key, r);
      }
    }
    return [...sourceTables.values()];
  }

  tableSyncsData(table: SourceTableInterface): boolean {
    for (let bucket of this.bucket_descriptors) {
      if (bucket.tableSyncsData(table)) {
        return true;
      }
    }
    return false;
  }

  tableSyncsParameters(table: SourceTableInterface): boolean {
    for (let bucket of this.bucket_descriptors) {
      if (bucket.tableSyncsParameters(table)) {
        return true;
      }
    }
    return false;
  }

  debugGetOutputTables() {
    let result: Record<string, any[]> = {};
    for (let bucket of this.bucket_descriptors) {
      for (let q of bucket.data_queries) {
        result[q.table!] ??= [];
        const r = {
          query: q.sql
        };

        result[q.table!].push(r);
      }
    }
    return result;
  }
}
