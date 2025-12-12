import { isScalar, LineCounter, parseDocument, Scalar, YAMLMap, YAMLSeq } from 'yaml';
import { isValidPriority } from './BucketDescription.js';
import { BucketParameterQuerier, QuerierError } from './BucketParameterQuerier.js';
import {
  BucketDataSource,
  ParameterIndexLookupCreator,
  BucketParameterQuerierSourceDefinition,
  BucketSource,
  CreateSourceParams
} from './BucketSource.js';
import {
  CompatibilityContext,
  CompatibilityEdition,
  CompatibilityOption,
  TimeValuePrecision
} from './compatibility.js';
import { SqlRuleError, SyncRulesErrors, YamlError } from './errors.js';
import { SqlEventDescriptor } from './events/SqlEventDescriptor.js';
import { DEFAULT_HYDRATION_STATE } from './HydrationState.js';
import { validateSyncRulesSchema } from './json_schema.js';
import { SourceTableInterface } from './SourceTableInterface.js';
import { QueryParseResult, SqlBucketDescriptor } from './SqlBucketDescriptor.js';
import { syncStreamFromSql } from './streams/from_sql.js';
import { HydratedSyncRules } from './HydratedSyncRules.js';
import { TablePattern } from './TablePattern.js';
import {
  QueryParseOptions,
  RequestParameters,
  SourceSchema,
  SqliteInputValue,
  SqliteJsonRow,
  SqliteRow,
  SqliteValue,
  StreamParseOptions
} from './types.js';
import { applyRowContext } from './utils.js';

const ACCEPT_POTENTIALLY_DANGEROUS_QUERIES = Symbol('ACCEPT_POTENTIALLY_DANGEROUS_QUERIES');

export interface SyncRulesOptions {
  schema?: SourceSchema;
  /**
   * The default schema to use when only a table name is specified.
   *
   * 'public' for Postgres, default database for MongoDB/MySQL.
   */
  defaultSchema: string;

  throwOnError?: boolean;
}

export interface RequestedStream {
  /**
   * The parameters for the explicit stream subscription.
   *
   * Unlike {@link GetQuerierOptions.globalParameters}, these parameters are only applied to the particular stream.
   */
  parameters: SqliteJsonRow | null;

  /**
   * An opaque id of the stream subscription, used to associate buckets with the stream subscriptions that have caused
   * them to be included.
   */
  opaque_id: number;
}

export interface GetQuerierOptions {
  globalParameters: RequestParameters;
  /**
   * Whether the client is subscribing to default query streams.
   *
   * Client do this by default, but can disable the behavior if needed.
   */
  hasDefaultStreams: boolean;
  /**
   *
   * For streams, this is invoked to check whether the client has opened the relevant stream.
   *
   * @param name The name of the stream as it appears in the sync rule definitions.
   * @returns If the strema has been opened by the client, the stream parameters for that particular stream. Otherwise
   * null.
   */
  streams: Record<string, RequestedStream[]>;
}

export interface GetBucketParameterQuerierResult {
  querier: BucketParameterQuerier;
  errors: QuerierError[];
}

export class SqlSyncRules {
  bucketDataSources: BucketDataSource[] = [];
  bucketParameterLookupSources: ParameterIndexLookupCreator[] = [];
  bucketParameterQuerierSources: BucketParameterQuerierSourceDefinition[] = [];
  bucketSources: BucketSource[] = [];

  eventDescriptors: SqlEventDescriptor[] = [];
  compatibility: CompatibilityContext = CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY;

  content: string;

  errors: YamlError[] = [];

  static validate(yaml: string, options: SyncRulesOptions): YamlError[] {
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

  static fromYaml(yaml: string, options: SyncRulesOptions) {
    const throwOnError = options.throwOnError ?? true;

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

    const declaredOptions = parsed.get('config') as YAMLMap | null;
    let compatibility = CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY;
    if (declaredOptions != null) {
      const edition = (declaredOptions.get('edition') ?? CompatibilityEdition.LEGACY) as CompatibilityEdition;
      const options = new Map<CompatibilityOption, boolean>();
      let maxTimeValuePrecision: TimeValuePrecision | undefined = undefined;

      for (const entry of declaredOptions.items) {
        const {
          key: { value: key },
          value: { value }
        } = entry as { key: Scalar<string>; value: Scalar<any> };

        if (key == 'timestamp_max_precision') {
          maxTimeValuePrecision = TimeValuePrecision.byName[value];
        }

        const option = CompatibilityOption.byName[key];
        if (option) {
          options.set(option, value);
        }
      }

      compatibility = new CompatibilityContext({ edition, overrides: options, maxTimeValuePrecision });
      if (maxTimeValuePrecision && !compatibility.isEnabled(CompatibilityOption.timestampsIso8601)) {
        rules.errors.push(
          new YamlError(new Error(`'timestamp_max_precision' requires 'timestamps_iso8601' to be enabled.`))
        );
      }

      rules.compatibility = compatibility;
    }

    // Bucket definitions using explicit parameter and data queries.
    const bucketMap = parsed.get('bucket_definitions') as YAMLMap;
    const streamMap = parsed.get('streams') as YAMLMap | null;
    const definitionNames = new Set<string>();
    const checkUniqueName = (name: string, literal: Scalar) => {
      if (definitionNames.has(name)) {
        rules.errors.push(this.tokenError(literal, 'Duplicate stream or bucket definition.'));
        return false;
      }

      definitionNames.add(name);
      return true;
    };

    if (bucketMap == null && streamMap == null) {
      rules.errors.push(new YamlError(new Error(`'bucket_definitions' or 'streams' is required`)));

      if (throwOnError) {
        rules.throwOnError();
      }
      return rules;
    }

    for (let entry of bucketMap?.items ?? []) {
      const { key: keyScalar, value } = entry as { key: Scalar; value: YAMLMap };
      const key = keyScalar.toString();
      if (!checkUniqueName(key, keyScalar)) {
        continue;
      }

      if (value == null || !(value instanceof YAMLMap)) {
        rules.errors.push(this.tokenError(keyScalar, `'${key}' bucket definition must be an object`));
        continue;
      }

      const accept_potentially_dangerous_queries =
        value.get('accept_potentially_dangerous_queries', true)?.value == true;
      const parseOptionPriority = rules.parsePriority(value);

      const queryOptions: QueryParseOptions = {
        ...options,
        accept_potentially_dangerous_queries,
        priority: parseOptionPriority,
        compatibility
      };
      const parameters = value.get('parameters', true) as unknown;
      const dataQueries = value.get('data', true) as unknown;

      const descriptor = new SqlBucketDescriptor(key);

      if (parameters instanceof Scalar) {
        rules.withScalar(parameters, (q) => {
          return descriptor.addParameterQuery(q, queryOptions);
        });
      } else if (parameters instanceof YAMLSeq) {
        for (let item of parameters.items) {
          rules.withScalar(item, (q) => {
            return descriptor.addParameterQuery(q, queryOptions);
          });
        }
      } else {
        descriptor.addParameterQuery('SELECT', queryOptions);
      }

      if (!(dataQueries instanceof YAMLSeq)) {
        rules.errors.push(this.tokenError(dataQueries ?? value, `'data' must be an array`));
        continue;
      }
      for (let query of dataQueries.items) {
        rules.withScalar(query, (q) => {
          return descriptor.addDataQuery(q, queryOptions, compatibility);
        });
      }

      rules.bucketSources.push(descriptor);
      rules.bucketDataSources.push(...descriptor.dataSources);
      rules.bucketParameterLookupSources.push(...descriptor.parameterIndexLookupCreators);
      rules.bucketParameterQuerierSources.push(...descriptor.parameterQuerierSources);
    }

    for (const entry of streamMap?.items ?? []) {
      const { key: keyScalar, value } = entry as { key: Scalar; value: YAMLMap };
      const key = keyScalar.toString();
      if (!checkUniqueName(key, keyScalar)) {
        continue;
      }

      const accept_potentially_dangerous_queries =
        value.get('accept_potentially_dangerous_queries', true)?.value == true;

      const queryOptions: StreamParseOptions = {
        ...options,
        accept_potentially_dangerous_queries,
        priority: rules.parsePriority(value),
        auto_subscribe: value.get('auto_subscribe', true)?.value == true,
        compatibility
      };

      const data = value.get('query', true) as unknown;
      if (data instanceof Scalar) {
        rules.withScalar(data, (q) => {
          const [parsed, errors] = syncStreamFromSql(key, q, queryOptions);
          rules.bucketSources.push(parsed);
          rules.bucketDataSources.push(...parsed.dataSources);
          rules.bucketParameterLookupSources.push(...parsed.parameterIndexLookupCreators);
          rules.bucketParameterQuerierSources.push(...parsed.parameterQuerierSources);
          return {
            parsed: true,
            errors
          };
        });
      } else {
        rules.errors.push(this.tokenError(data, 'Must be a string.'));
        continue;
      }
    }

    const eventMap = parsed.get('event_definitions') as YAMLMap;
    for (const event of eventMap?.items ?? []) {
      const { key, value } = event as { key: Scalar; value: YAMLSeq };

      if (false == value instanceof YAMLMap) {
        rules.errors.push(new YamlError(new Error(`Event definitions must be objects.`)));
        continue;
      }

      const payloads = value.get('payloads') as YAMLSeq;
      if (false == payloads instanceof YAMLSeq) {
        rules.errors.push(new YamlError(new Error(`Event definition payloads must be an array.`)));
        continue;
      }

      const eventDescriptor = new SqlEventDescriptor(key.toString(), compatibility);
      for (let item of payloads.items) {
        if (!isScalar(item)) {
          rules.errors.push(new YamlError(new Error(`Payload queries for events must be scalar.`)));
          continue;
        }
        rules.withScalar(item, (q) => {
          return eventDescriptor.addSourceQuery(q, options);
        });
      }

      rules.eventDescriptors.push(eventDescriptor);
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
   * Hydrate the sync rule definitions with persisted state into runnable sync rules.
   *
   * @param params.hydrationState Transforms bucket ids based on persisted state. May omit for tests.
   */
  hydrate(params?: CreateSourceParams): HydratedSyncRules {
    let hydrationState = params?.hydrationState;
    if (hydrationState == null || !this.compatibility.isEnabled(CompatibilityOption.versionedBucketIds)) {
      hydrationState = DEFAULT_HYDRATION_STATE;
    }
    const resolvedParams = { hydrationState };
    return new HydratedSyncRules({
      definition: this,
      createParams: resolvedParams,
      bucketDataSources: this.bucketDataSources,
      bucketParameterIndexLookupCreators: this.bucketParameterLookupSources,
      eventDescriptors: this.eventDescriptors,
      compatibility: this.compatibility
    });
  }

  applyRowContext<MaybeToast extends undefined = never>(
    source: SqliteRow<SqliteInputValue | MaybeToast>
  ): SqliteRow<SqliteValue | MaybeToast> {
    return applyRowContext(source, this.compatibility);
  }

  getSourceTables(): TablePattern[] {
    const sourceTables = new Map<String, TablePattern>();
    for (const bucket of this.bucketDataSources) {
      for (const r of bucket.getSourceTables()) {
        const key = `${r.connectionTag}.${r.schema}.${r.tablePattern}`;
        sourceTables.set(key, r);
      }
    }
    for (const bucket of this.bucketParameterLookupSources) {
      for (const r of bucket.getSourceTables()) {
        const key = `${r.connectionTag}.${r.schema}.${r.tablePattern}`;
        sourceTables.set(key, r);
      }
    }

    for (const event of this.eventDescriptors) {
      for (const r of event.getSourceTables()) {
        const key = `${r.connectionTag}.${r.schema}.${r.tablePattern}`;
        sourceTables.set(key, r);
      }
    }

    return [...sourceTables.values()];
  }

  getEventTables(): TablePattern[] {
    const eventTables = new Map<String, TablePattern>();

    for (const event of this.eventDescriptors) {
      for (const r of event.getSourceTables()) {
        const key = `${r.connectionTag}.${r.schema}.${r.tablePattern}`;
        eventTables.set(key, r);
      }
    }
    return [...eventTables.values()];
  }

  tableTriggersEvent(table: SourceTableInterface): boolean {
    return this.eventDescriptors.some((bucket) => bucket.tableTriggersEvent(table));
  }

  tableSyncsData(table: SourceTableInterface): boolean {
    return this.bucketDataSources.some((b) => b.tableSyncsData(table));
  }

  tableSyncsParameters(table: SourceTableInterface): boolean {
    return this.bucketParameterLookupSources.some((b) => b.tableSyncsParameters(table));
  }

  debugGetOutputTables() {
    let result: Record<string, any[]> = {};
    for (let bucket of this.bucketDataSources) {
      bucket.debugWriteOutputTables(result);
    }
    return result;
  }

  debugRepresentation() {
    return this.bucketSources.map((rules) => rules.debugRepresentation());
  }

  private parsePriority(value: YAMLMap) {
    if (value.has('priority')) {
      const priorityValue = value.get('priority', true)!;
      if (typeof priorityValue.value != 'number' || !isValidPriority(priorityValue.value)) {
        this.errors.push(
          SqlSyncRules.tokenError(priorityValue, 'Invalid priority, expected a number between 0 and 3 (inclusive).')
        );
      } else {
        return priorityValue.value;
      }
    }
  }
}
