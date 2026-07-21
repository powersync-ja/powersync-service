import { LineCounter, parseDocument, Scalar } from 'yaml';
import { DEFAULT_BUCKET_PRIORITY, isValidPriority } from './BucketDescription.js';
import {
  CompatibilityContext,
  CompatibilityEdition,
  CompatibilityOption,
  TimeValuePrecision
} from './compatibility.js';
import { ParsingErrorListener, SyncStreamsCompiler } from './compiler/compiler.js';
import { CommonTableExpression } from './compiler/sqlite.js';
import { SqlRuleError, SyncRulesErrors, YamlError } from './errors.js';
import { SqlEventDescriptor } from './events/SqlEventDescriptor.js';
import { QueryParseResult, SqlBucketDescriptor } from './legacy/SqlBucketDescriptor.js';
import { syncStreamFromSql } from './legacy/streams/from_sql.js';
import { SqlSyncRules } from './SqlSyncRules.js';
import { validateStorageVersion } from './StorageVersion.js';
import { PrecompiledSyncConfig } from './sync_plan/evaluator/index.js';
import { SyncConfig, SyncConfigWithErrors } from './SyncConfig.js';
import { TablePattern } from './TablePattern.js';
import { QueryParseOptions, SourceSchema, StreamParseOptions } from './types.js';
import { buildParsedToSourceValueMap, isBlockScalar, isQuotedScalar } from './yaml_scalar_map.js';
import { documentState, YamlMapState, YamlScalarState, YamlState } from './yaml_validation.js';

const ACCEPT_POTENTIALLY_DANGEROUS_QUERIES = Symbol('ACCEPT_POTENTIALLY_DANGEROUS_QUERIES');

/**
 * Reads `sync_rules.yaml` files containing a sync configuration.
 *
 * @internal Only exposed through `SqlSyncRules.fromYaml`.
 */
export class SyncConfigFromYaml {
  readonly #errors: YamlError[] = [];
  readonly #lineCounter = new LineCounter();

  // Names of bucket definitions and sync streams, to prevent duplicates.
  readonly #definitionNames = new Set<string>();

  readonly #definedCtes: CommonTableExpressionWithName[] = [];

  constructor(
    private readonly options: SyncConfigFromYamlOptions,
    private readonly yaml: string
  ) {}

  read(): SyncConfigWithErrors {
    return { config: this.#read(), errors: this.#errors };
  }

  #read(): SyncConfig {
    const parsed = parseDocument(this.yaml, {
      schema: 'core',
      keepSourceTokens: true,
      lineCounter: this.#lineCounter,
      customTags: [
        {
          tag: '!accept_potentially_dangerous_queries',
          resolve(_text: string, _onError: (error: string) => void) {
            return ACCEPT_POTENTIALLY_DANGEROUS_QUERIES;
          }
        }
      ]
    });

    const rootState = documentState(parsed, (e) => this.#errors.push(e)).requireMap();

    if (parsed.errors.length > 0 || rootState == null) {
      this.#errors.push(...parsed.errors.map((e) => new YamlError(e)));
      this.#throwOnErrorIfRequested();

      // Return an empty sync config instance if we couldn't parse YAML, it doesn't make sense to try parsing the broken
      // structure further.
      return new SqlSyncRules(this.yaml);
    }

    using declaredOptions = rootState.get('config')?.requireMap();
    let compatibility: CompatibilityContext;
    let storageVersion: number | undefined;
    if (declaredOptions) {
      compatibility = this.#parseCompatibilityOptions(declaredOptions);
      storageVersion = this.#validateStorageVersion(declaredOptions);

      if (compatibility.isEnabled(CompatibilityOption.sqliteExpressionEngine)) {
        // Evaluating expressions with SQLite requires edition: 3, older systems always use JavaScript.
        if (compatibility.edition < CompatibilityEdition.SYNC_STREAMS) {
          declaredOptions.reportError('Enabling unstable_sqlite_expression_engine requires edition: 3');
        } else if (!compatibility.isEnabled(CompatibilityOption.fixedJsonExtract)) {
          declaredOptions.reportError('Enabling unstable_sqlite_expression_engine requires fixed_json_extract');
        }
      }
    } else {
      compatibility = CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY;
    }

    // Bucket definitions using explicit parameter and data queries.
    const bucketMap = rootState.get('bucket_definitions')?.requireMap();
    const streamMap = rootState.get('streams')?.requireMap();
    const globalCtes = rootState.get('with')?.requireMap();

    let result: SyncConfig;
    if (compatibility.edition >= CompatibilityEdition.COMPILED_STREAMS) {
      result = this.#compileSyncPlan(bucketMap, streamMap, globalCtes, compatibility);
      this.#warnOnUnusedCtes();
    } else {
      // We don't support CTEs at all in this compiler implementation.
      globalCtes?.reportError('Common table expressions require edition 3.');

      result = this.#legacyParseBucketDefinitionsAndStreams(bucketMap, streamMap, compatibility);
    }

    result.storageVersion = storageVersion;

    const eventDefinitions = this.#parseEventDefinitions(rootState, compatibility);
    result.eventDescriptors.push(...eventDefinitions);

    this.#throwOnErrorIfRequested();
    return result;
  }

  get #hasFatalError(): boolean {
    return this.#errors.find((e) => e.type != 'warning') != null;
  }

  #throwOnErrorIfRequested() {
    if (this.options.throwOnError && this.#hasFatalError) {
      throw new SyncRulesErrors(this.#errors);
    }
  }

  /**
   * Parses the `config` block of a sync configuration.
   *
   * @see https://docs.powersync.com/sync/advanced/compatibility
   */
  #parseCompatibilityOptions(declaredOptions: YamlMapState) {
    const edition = (declaredOptions.get('edition')?.requireScalar()?.requireNumeric() ??
      CompatibilityEdition.LEGACY) as CompatibilityEdition;
    const options = new Map<CompatibilityOption, boolean>();
    let maxTimeValuePrecision: TimeValuePrecision | undefined = undefined;

    for (const [key, option] of Object.entries(CompatibilityOption.byName)) {
      const isEnabled = declaredOptions.get(key)?.requireScalar()?.requireBoolean();
      if (isEnabled != null) {
        options.set(option, isEnabled);
      }
    }

    const rawMaxTimestampPrecision = declaredOptions.get('timestamp_max_precision')?.requireScalar()?.requireString();
    if (rawMaxTimestampPrecision != null) {
      maxTimeValuePrecision = TimeValuePrecision.byName[rawMaxTimestampPrecision];
    }

    const compatibility = new CompatibilityContext({ edition, overrides: options, maxTimeValuePrecision });
    if (maxTimeValuePrecision && !compatibility.isEnabled(CompatibilityOption.timestampsIso8601)) {
      declaredOptions.reportError(`'timestamp_max_precision' requires 'timestamps_iso8601' to be enabled.`);
    }

    return compatibility;
  }

  #compileSyncPlan(
    bucketMap: YamlMapState | undefined,
    streamMap: YamlMapState | undefined,
    globalCtes: YamlMapState | undefined,
    compatibility: CompatibilityContext
  ) {
    bucketMap?.reportError(
      `'bucket_definitions' are not supported by the new compiler. Consider using https://powersync-community.github.io/bucket-definitions-to-sync-streams/ to translate them to streams.`
    );

    if (streamMap == null) {
      this.#errors.push(new YamlError(new Error(`'streams' are required.`)));
    }

    const compiler = new SyncStreamsCompiler(this.options);

    const parseCommonTableExpressions = (from: YamlState | undefined | null): Map<string, CommonTableExpression> => {
      const map = new Map<string, CommonTableExpression>();
      const fromMap = from?.requireMap();
      if (fromMap != null) {
        for (const { key: cteName, keyScalar, value } of fromMap.stringKeyedItems()) {
          const cteQuery = value.requireScalar();
          if (this.options.schema) {
            // Emit a warning if the CTE shadows a name from the schema.
            const pattern = new TablePattern(this.options.defaultSchema, cteName);
            if (this.options.schema.getTables(pattern)?.length > 0) {
              keyScalar.reportError(
                'This common table expression shadows the name of a table in the source schema.',
                'warning'
              );
            }
          }

          if (cteQuery) {
            const [sql, errorListener] = this.#scalarErrorListener(cteQuery.node);
            const parsed = compiler.commonTableExpression(sql, errorListener);
            if (parsed) {
              const cte = { subquery: parsed, used: false };
              this.#definedCtes.push({ cte, name: keyScalar });
              map.set(cteName, cte);
            }
          }
        }
      }

      return map;
    };

    const parsedGlobalCommonTableExpressions = parseCommonTableExpressions(globalCtes);

    for (const { key, keyScalar, value: maybeMap } of streamMap?.stringKeyedItems() ?? []) {
      if (!this.#checkUniqueName(key, keyScalar)) {
        continue;
      }

      using value = maybeMap.requireMap();
      if (value == null) continue;

      const streamCompiler = compiler.stream({
        name: key,
        isSubscribedByDefault: value.get('auto_subscribe')?.requireScalar()?.requireBoolean() == true,
        priority: this.#parsePriority(value) ?? DEFAULT_BUCKET_PRIORITY,
        warnOnDangerousParameter: !this.#acceptPotentiallyUnsafeQueries(value)
      });
      parsedGlobalCommonTableExpressions.forEach((query, name) =>
        streamCompiler.registerCommonTableExpression(name, query)
      );

      // Add stream-local CTEs, which shadow global definitions.

      parseCommonTableExpressions(value.get('with')).forEach((query, name) =>
        streamCompiler.registerCommonTableExpression(name, query)
      );

      const addQuery = (query: YamlState) => {
        const scalar = query.requireScalar();
        if (scalar) {
          const [sql, errorListener] = this.#scalarErrorListener(scalar.node);
          streamCompiler.addQuery(sql, errorListener);
        }
      };

      const queries = value.get('queries');
      const query = value.get('query');

      if ((queries == null) == (query == null)) {
        value.reportError('One of `queries` or `query` must be given.');
      }
      if (query) {
        addQuery(query);
      }
      if (queries) {
        for (const queryEntry of queries?.requireSequence()?.items ?? []) {
          addQuery(queryEntry);
        }
      }

      streamCompiler.finish();
    }

    // We pass an empty array for eventDefinitions here because those will get parsed in #parseEventDefinitions.
    return new PrecompiledSyncConfig(compiler.output.toSyncPlan(), compatibility, [], {
      defaultSchema: this.options.defaultSchema,
      sourceText: this.yaml
    });
  }

  #warnOnUnusedCtes() {
    for (const { cte, name } of this.#definedCtes) {
      if (!cte.used) {
        name.reportError(`This common table expression isn't referenced.`, 'warning');
      }
    }
  }

  #legacyParseBucketDefinitionsAndStreams(
    bucketMap: YamlMapState | undefined,
    streamMap: YamlMapState | undefined,
    compatibility: CompatibilityContext
  ) {
    const rules = new SqlSyncRules(this.yaml);
    rules.compatibility = compatibility;

    if (bucketMap == null && streamMap == null) {
      this.#errors.push(new YamlError(new Error(`'bucket_definitions' or 'streams' is required`)));
      this.#throwOnErrorIfRequested();
    }

    // This is with config.edition <= 2, we want to encourage users with streams to migrate to version 3 to use
    // compiled sync plans.
    streamMap?.reportError(
      'This is using an alpha version of Sync Streams. We recommend upgrading `config.edition` to version 3 to support the latest features.',
      'warning'
    );

    for (const { key, keyScalar, value: maybeMap } of bucketMap?.stringKeyedItems() ?? []) {
      if (!this.#checkUniqueName(key, keyScalar)) {
        continue;
      }

      using value = maybeMap.requireMap();
      if (value == null) continue;

      const accept_potentially_dangerous_queries = this.#acceptPotentiallyUnsafeQueries(value);
      const parseOptionPriority = this.#parsePriority(value);

      const queryOptions: QueryParseOptions = {
        ...this.options,
        accept_potentially_dangerous_queries,
        priority: parseOptionPriority,
        compatibility
      };
      const parameters = value.get('parameters');
      const dataQueries = value.require('data')?.requireSequence();

      const descriptor = new SqlBucketDescriptor(key);

      if (parameters == null) {
        descriptor.addParameterQuery('SELECT', queryOptions);
      } else if (parameters.node instanceof Scalar) {
        this.#withScalar(parameters, (q) => {
          return descriptor.addParameterQuery(q, queryOptions);
        });
      } else {
        const seq = parameters.requireSequence('Parameters must be a string or array of strings');
        if (seq != null) {
          for (let item of seq.items) {
            this.#withScalar(item, (q) => {
              return descriptor.addParameterQuery(q, queryOptions);
            });
          }
        }
      }

      if (dataQueries == null) {
        continue;
      }
      for (let query of dataQueries.items) {
        this.#withScalar(query, (q) => {
          return descriptor.addDataQuery(q, queryOptions, compatibility);
        });
      }

      rules.bucketSources.push(descriptor);
      rules.bucketDataSources.push(...descriptor.dataSources);
      rules.bucketParameterLookupSources.push(...descriptor.parameterIndexLookupCreators);
    }

    for (const { key, keyScalar, value } of streamMap?.stringKeyedItems() ?? []) {
      if (!this.#checkUniqueName(key, keyScalar)) {
        continue;
      }

      using map = value.requireMap();
      if (map == null) continue;

      // We don't support with or multiple queries in streams, those are only supported by the new compiler.
      map
        .get('with')
        ?.reportError('Common table expressions are not supported without the `sync_config_compiler` option.');
      map.get('queries')?.reportError('Multiple queries not supported without the `sync_config_compiler` option.');

      const accept_potentially_dangerous_queries =
        map.get('accept_potentially_dangerous_queries')?.requireScalar()?.requireBoolean() == true;

      const queryOptions: StreamParseOptions = {
        ...this.options,
        accept_potentially_dangerous_queries,
        priority: this.#parsePriority(map),
        auto_subscribe: map.get('auto_subscribe')?.requireScalar()?.requireBoolean() == true,
        compatibility
      };

      const data = map.get('query')?.requireScalar('Must be a string');
      if (data != null) {
        this.#withScalar(data, (q) => {
          const [parsed, errors] = syncStreamFromSql(key, q, queryOptions);
          rules.bucketSources.push(parsed);
          rules.bucketDataSources.push(...parsed.dataSources);
          rules.bucketParameterLookupSources.push(...parsed.parameterIndexLookupCreators);
          return {
            parsed: true,
            errors
          };
        });
      }
    }

    return rules;
  }

  #validateStorageVersion(config: YamlMapState): number | undefined {
    const storageScalar = config.get('storage_version')?.requireScalar();
    if (storageScalar != null) {
      const rawVersion = storageScalar.requireNumeric('Storage version must be numeric');

      if (rawVersion != null) {
        const version = validateStorageVersion(rawVersion);
        if (version == null) {
          storageScalar.reportError(`Storage version ${rawVersion} is not supported`);
        } else if (!version.stable) {
          storageScalar.reportError(
            `Storage version ${version.version} is unstable, and may cause unexpected behavior or stop functioning in any release`,
            'warning'
          );
        }
        return version?.version;
      }
    }
    return undefined;
  }

  #parseEventDefinitions(parsed: YamlMapState, compatibility: CompatibilityContext) {
    const eventMap = parsed.get('event_definitions')?.requireMap();
    const eventDescriptors: SqlEventDescriptor[] = [];

    for (const { key: name, keyScalar, value: maybeMap } of eventMap?.stringKeyedItems() ?? []) {
      using value = maybeMap.requireMap(`Event definitions must be objects.`);
      if (value == null) continue;

      const payloads = value.get('payloads')?.requireSequence(`Event definition payloads must be an array.`);
      if (payloads == null) continue;

      const eventDescriptor = new SqlEventDescriptor(name, compatibility);
      for (let item of payloads.items) {
        const itemScalar = item.requireScalar(`Payload queries for events must be scalar.`);
        if (itemScalar == null) continue;

        this.#withScalar(item, (q) => {
          return eventDescriptor.addSourceQuery(q, this.options);
        });
      }

      eventDescriptors.push(eventDescriptor);
    }

    return eventDescriptors;
  }

  #checkUniqueName(name: string, literal: YamlState): boolean {
    if (this.#definitionNames.has(name)) {
      literal.reportError('Duplicate stream or bucket definition.');
      return false;
    }

    this.#definitionNames.add(name);
    return true;
  }

  #parsePriority(value: YamlMapState) {
    const message = 'Invalid priority, expected a number between 0 and 3 (inclusive).';
    const priorityScalar = value.get('priority')?.requireScalar();
    const rawPriority = priorityScalar?.requireNumeric(message);

    if (priorityScalar == null || rawPriority == null) return;
    if (!isValidPriority(rawPriority)) {
      priorityScalar.reportError(message);
      return;
    }

    return rawPriority;
  }

  #acceptPotentiallyUnsafeQueries(definition: YamlMapState): boolean {
    return definition.get('accept_potentially_dangerous_queries')?.requireScalar()?.requireBoolean() ?? false;
  }

  #tokenError(token: Scalar, message: string) {
    const start = token?.srcToken?.offset ?? 0;
    const end = start + 1;
    return new YamlError(new Error(message), { start, end });
  }

  #withScalar(state: YamlState, cb: (value: string) => QueryParseResult): void {
    const scalar = state.requireScalar();
    if (scalar == null) {
      return;
    }

    const value = scalar.node.toString();

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
      this.#addErrorFromScalar(scalar.node, value, err);
    }
    return;
  }

  /**
   * Reads string contents from a YAML scalar and returns an error listener for the sync stream compiler.
   */
  #scalarErrorListener(scalar: Scalar): [string, ParsingErrorListener] {
    const value = scalar.toString();
    const listener: ParsingErrorListener = {
      report: (message, location, options): void => {
        const error = new SqlRuleError(message, value, location);
        if (options?.isWarning) {
          error.type = 'warning';
        }

        this.#addErrorFromScalar(scalar, value, error);
      }
    };

    return [value, listener];
  }

  /**
   * Adds an error originally added while parsing a YAML scalar string.
   *
   * @param scalar The scalar being parsed.
   * @param value String contents of the scalar.
   * @param error An error in the scalar content. Offsets will be translated to point at the full YAML source.
   */
  #addErrorFromScalar(scalar: Scalar, value: string, err: SqlRuleError) {
    const srcToken = scalar.srcToken!;
    // For block scalars, skip past the | or > header line. For quoted scalars, skip the opening quote.
    const valueStart = isBlockScalar(scalar.type)
      ? this.yaml.indexOf('\n', srcToken.offset) + 1
      : srcToken.offset + (isQuotedScalar(scalar.type) ? 1 : 0);

    let offset: number;
    let end: number;

    // Build an offset map to translate parsed-value positions to source positions, handling
    // escape sequences in quoted scalars and stripped indentation in block scalars.
    const valueSource = isQuotedScalar(scalar.type)
      ? this.yaml.slice(valueStart, scalar.range![1] - 1)
      : this.yaml.slice(valueStart, scalar.range![1]);
    const offsetMap = buildParsedToSourceValueMap(valueSource, scalar.type);

    if (err instanceof SqlRuleError && err.location) {
      offset = valueStart + (offsetMap[err.location.start] ?? err.location.start);
      end = valueStart + (offsetMap[err.location.end] ?? err.location.end);
    } else if (typeof (err as any).token?._location?.start == 'number') {
      const rawStart = (err as any).token._location.start;
      const rawEnd = (err as any).token._location.end;
      offset = valueStart + (offsetMap[rawStart] ?? rawStart);
      end = valueStart + (offsetMap[rawEnd] ?? rawEnd);
    } else {
      offset = valueStart;
      end = valueStart + Math.max(value.length, 1);
    }

    const pos = { start: offset, end };
    this.#errors.push(new YamlError(err, pos));
  }
}

export interface SyncConfigFromYamlOptions {
  readonly throwOnError: boolean;
  readonly schema?: SourceSchema;
  /**
   * The default schema to use when only a table name is specified.
   *
   * 'public' for Postgres, default database for MongoDB/MySQL.
   */
  readonly defaultSchema: string;
}

interface CommonTableExpressionWithName {
  // The key in a `with` map defining the CTE.
  name: YamlScalarState;
  cte: CommonTableExpression;
}
