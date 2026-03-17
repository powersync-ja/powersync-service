import { Document, isScalar, LineCounter, Node, parseDocument, Scalar, YAMLMap, YAMLSeq } from 'yaml';
import { SqlRuleError, SyncRulesErrors, YamlError } from './errors.js';
import { DEFAULT_BUCKET_PRIORITY, isValidPriority } from './BucketDescription.js';
import {
  CompatibilityContext,
  CompatibilityEdition,
  CompatibilityOption,
  TimeValuePrecision
} from './compatibility.js';
import { SqlSyncRules } from './SqlSyncRules.js';
import { validateSyncRulesSchema } from './json_schema.js';
import { SqlEventDescriptor } from './events/SqlEventDescriptor.js';
import { QueryParseResult, SqlBucketDescriptor } from './SqlBucketDescriptor.js';
import { QueryParseOptions, SourceSchema, StreamParseOptions } from './types.js';
import { SyncConfig, SyncConfigWithErrors } from './SyncConfig.js';
import { ParsingErrorListener, SyncStreamsCompiler } from './compiler/compiler.js';
import { syncStreamFromSql } from './streams/from_sql.js';
import { PrecompiledSyncConfig } from './sync_plan/evaluator/index.js';
import { javaScriptExpressionEngine } from './sync_plan/engine/javascript.js';
import { PreparedSubquery } from './compiler/sqlite.js';
import { TablePattern } from './TablePattern.js';

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

    if (parsed.errors.length > 0) {
      this.#errors.push(...parsed.errors.map((e) => new YamlError(e)));
      this.#throwOnErrorIfRequested();

      // Return an empty sync rules instance if we couldn't parse YAML, it doesn't make sense to try parsing the broken
      // structure further.
      return new SqlSyncRules(this.yaml);
    }

    let compatibility: CompatibilityContext;
    if (parsed.has('config')) {
      const declaredOptions = parsed.get('config') as YAMLMap;
      compatibility = this.#parseCompatibilityOptions(declaredOptions);
    } else {
      compatibility = CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY;
    }

    // Bucket definitions using explicit parameter and data queries.
    const bucketMap = parsed.get('bucket_definitions') as YAMLMap | null;
    const streamMap = parsed.get('streams') as YAMLMap | null;
    const globalCtes = parsed.get('with') as YAMLMap | null;

    let result: SyncConfig;
    if (compatibility.edition >= CompatibilityEdition.COMPILED_STREAMS) {
      result = this.#compileSyncPlan(bucketMap, streamMap, globalCtes, compatibility);
    } else {
      if (globalCtes != null) {
        // We don't support CTEs at all in this compiler implementation.
        this.#errors.push(
          this.#yamlError(
            globalCtes as Node,
            'Common table expressions are not supported without the `sync_config_compiler` option.'
          )
        );
      }

      result = this.#legacyParseBucketDefinitionsAndStreams(bucketMap, streamMap, compatibility);
    }

    const eventDefinitions = this.#parseEventDefinitions(parsed, compatibility);
    result.eventDescriptors.push(...eventDefinitions);

    // Validate that there are no additional properties.
    // Since these errors don't contain line numbers, do this last.
    const valid = validateSyncRulesSchema(parsed.toJSON());
    if (!valid) {
      this.#errors.push(
        ...validateSyncRulesSchema.errors!.map((e: any) => {
          return new YamlError(e);
        })
      );
    }
    this.#throwOnErrorIfRequested();
    return result;
  }

  #throwOnErrorIfRequested() {
    if (this.options.throwOnError && this.#errors.find((e) => e.type != 'warning') != null) {
      throw new SyncRulesErrors(this.#errors);
    }
  }

  /**
   * Parses the `config` block of a sync configuration.
   *
   * @see https://docs.powersync.com/sync/advanced/compatibility
   */
  #parseCompatibilityOptions(declaredOptions: YAMLMap) {
    const edition = (declaredOptions.get('edition') ?? CompatibilityEdition.LEGACY) as CompatibilityEdition;
    const options = new Map<CompatibilityOption, boolean>();
    let maxTimeValuePrecision: TimeValuePrecision | undefined = undefined;
    let useNewCompiler = false;

    for (const entry of declaredOptions.items) {
      const {
        key: { value: key },
        value: { value }
      } = entry as { key: Scalar<string>; value: Scalar<any> };

      if (key == 'timestamp_max_precision') {
        maxTimeValuePrecision = TimeValuePrecision.byName[value];
      }

      if (key == 'sync_config_compiler') {
        useNewCompiler = Boolean(value);
        continue;
      }

      const option = CompatibilityOption.byName[key];
      if (option) {
        options.set(option, Boolean(value));
      }
    }

    const compatibility = new CompatibilityContext({ edition, overrides: options, maxTimeValuePrecision });
    if (maxTimeValuePrecision && !compatibility.isEnabled(CompatibilityOption.timestampsIso8601)) {
      this.#errors.push(
        new YamlError(new Error(`'timestamp_max_precision' requires 'timestamps_iso8601' to be enabled.`))
      );
    }

    return compatibility;
  }

  #compileSyncPlan(
    bucketMap: YAMLMap | null,
    streamMap: YAMLMap | null,
    globalCtes: YAMLMap | null,
    compatibility: CompatibilityContext
  ) {
    if (bucketMap != null) {
      this.#errors.push(
        this.#yamlError(
          bucketMap,
          `'bucket_definitions' are not supported by the new compiler. Consider using https://powersync-community.github.io/bucket-definitions-to-sync-streams/ to translate them to streams.`
        )
      );
    }
    if (streamMap == null) {
      this.#errors.push(new YamlError(new Error(`'streams' are required.`)));
    }

    const compiler = new SyncStreamsCompiler(this.options);

    const parseCommonTableExpressions = (from: YAMLMap | null): Map<string, PreparedSubquery> => {
      const map = new Map();
      if (from != null) {
        for (const entry of from.items ?? []) {
          const { key: cteNameScalar, value: cteQuery } = entry as { key: Scalar<string>; value: Scalar };
          const cteName = cteNameScalar.value;

          if (this.options.schema) {
            // Emit a warning if the CTE shadows a name from the schema.
            const pattern = new TablePattern(this.options.defaultSchema, cteName);
            if (this.options.schema.getTables(pattern)?.length > 0) {
              const error = this.#yamlError(
                cteNameScalar,
                'This common table expression shadows the name of a table in the source schema.'
              );
              error.type = 'warning';
              this.#errors.push(error);
            }
          }

          const [sql, errorListener] = this.#scalarErrorListener(cteQuery);
          const parsed = compiler.commonTableExpression(sql, errorListener);
          if (parsed) {
            map.set(cteName, parsed);
          }
        }
      }

      return map;
    };

    const parsedGlobalCommonTableExpressions = parseCommonTableExpressions(globalCtes);

    for (const entry of streamMap?.items ?? []) {
      const { key: keyScalar, value } = entry as { key: Scalar; value: YAMLMap };
      if (!(value instanceof YAMLMap)) {
        // The json schema validator will flag this later.
        continue;
      }

      const key = keyScalar.toString();
      if (!this.#checkUniqueName(key, keyScalar)) {
        continue;
      }

      const streamCompiler = compiler.stream({
        name: key,
        isSubscribedByDefault: value.get('auto_subscribe', true)?.value == true,
        priority: this.#parsePriority(value) ?? DEFAULT_BUCKET_PRIORITY,
        warnOnDangerousParameter: !this.#acceptPotentiallyUnsafeQueries(value)
      });
      parsedGlobalCommonTableExpressions.forEach((query, name) =>
        streamCompiler.registerCommonTableExpression(name, query)
      );

      // Add stream-local CTEs, which shadow global definitions.
      parseCommonTableExpressions(value.get('with') as YAMLMap | null).forEach((query, name) =>
        streamCompiler.registerCommonTableExpression(name, query)
      );

      const addQuery = (query: Scalar<string>) => {
        const [sql, errorListener] = this.#scalarErrorListener(query);
        streamCompiler.addQuery(sql, errorListener);
      };

      const queries = value.get('queries') as YAMLSeq | null;
      const query = value.get('query', true) as Scalar<string> | null;

      if ((queries == null) == (query == null)) {
        this.#errors.push(this.#yamlError(value, 'One of `queries` or `query` must be given.'));
      }
      if (query) {
        addQuery(query);
      }
      if (queries) {
        for (const queryEntry of queries.items) {
          if (queryEntry instanceof Scalar) {
            addQuery(queryEntry as Scalar<string>);
          }
        }
      }

      streamCompiler.finish();
    }

    // We pass an empty array for eventDefinitions here because those will get parsed in #parseEventDefinitions.
    return new PrecompiledSyncConfig(compiler.output.toSyncPlan(), compatibility, [], {
      defaultSchema: this.options.defaultSchema,
      engine: javaScriptExpressionEngine(compatibility),
      sourceText: this.yaml
    });
  }

  #legacyParseBucketDefinitionsAndStreams(
    bucketMap: YAMLMap | null,
    streamMap: YAMLMap | null,
    compatibility: CompatibilityContext
  ) {
    const rules = new SqlSyncRules(this.yaml);
    rules.compatibility = compatibility;

    if (bucketMap == null && streamMap == null) {
      this.#errors.push(new YamlError(new Error(`'bucket_definitions' or 'streams' is required`)));
      this.#throwOnErrorIfRequested();
    }

    if (streamMap != null) {
      // This is with config.edition <= 2, we want to encourage users with streams to migrate to version 3 to use
      // compiled sync plans.
      const error = this.#yamlError(
        streamMap,
        'This is using an alpha version of Sync Streams. We recommend upgrading `config.edition` to version 3 to support the latest features.'
      );
      error.type = 'warning';
      this.#errors.push(error);
    }

    for (let entry of bucketMap?.items ?? []) {
      const { key: keyScalar, value } = entry as { key: Scalar; value: YAMLMap };
      const key = keyScalar.toString();
      if (!this.#checkUniqueName(key, keyScalar)) {
        continue;
      }

      if (value == null || !(value instanceof YAMLMap)) {
        this.#errors.push(this.#tokenError(keyScalar, `'${key}' bucket definition must be an object`));
        continue;
      }

      const accept_potentially_dangerous_queries = this.#acceptPotentiallyUnsafeQueries(value);
      const parseOptionPriority = this.#parsePriority(value);

      const queryOptions: QueryParseOptions = {
        ...this.options,
        accept_potentially_dangerous_queries,
        priority: parseOptionPriority,
        compatibility
      };
      const parameters = value.get('parameters', true) as unknown;
      const dataQueries = value.get('data', true) as unknown;

      const descriptor = new SqlBucketDescriptor(key);

      if (parameters instanceof Scalar) {
        this.#withScalar(parameters, (q) => {
          return descriptor.addParameterQuery(q, queryOptions);
        });
      } else if (parameters instanceof YAMLSeq) {
        for (let item of parameters.items) {
          this.#withScalar(item, (q) => {
            return descriptor.addParameterQuery(q, queryOptions);
          });
        }
      } else {
        descriptor.addParameterQuery('SELECT', queryOptions);
      }

      if (!(dataQueries instanceof YAMLSeq)) {
        this.#errors.push(this.#tokenError((dataQueries ?? value) as any, `'data' must be an array`));
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

    for (const entry of streamMap?.items ?? []) {
      const { key: keyScalar, value } = entry as { key: Scalar; value: YAMLMap };
      const key = keyScalar.toString();
      if (!this.#checkUniqueName(key, keyScalar)) {
        continue;
      }

      // We don't support with or multiple queries in streams, those are only supported by the new compiler.
      const $with = value.get('with');
      if ($with != null) {
        this.#errors.push(
          this.#yamlError(
            $with as Node,
            'Common table expressions are not supported without the `sync_config_compiler` option.'
          )
        );
      }
      const queries = value.get('queries');
      if (queries != null) {
        this.#errors.push(
          this.#yamlError(queries as Node, 'Multiple queries not supported without the `sync_config_compiler` option.')
        );
      }

      const accept_potentially_dangerous_queries =
        value.get('accept_potentially_dangerous_queries', true)?.value == true;

      const queryOptions: StreamParseOptions = {
        ...this.options,
        accept_potentially_dangerous_queries,
        priority: this.#parsePriority(value),
        auto_subscribe: value.get('auto_subscribe', true)?.value == true,
        compatibility
      };

      const data = value.get('query', true) as unknown;
      if (data instanceof Scalar) {
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
      } else {
        this.#errors.push(this.#tokenError(data as any, 'Must be a string.'));
        continue;
      }
    }

    return rules;
  }

  #parseEventDefinitions(parsed: Document, compatibility: CompatibilityContext) {
    const eventMap = parsed.get('event_definitions') as YAMLMap;
    const eventDescriptors: SqlEventDescriptor[] = [];

    for (const event of eventMap?.items ?? []) {
      const { key, value } = event as { key: Scalar; value: YAMLSeq };

      if (false == value instanceof YAMLMap) {
        this.#errors.push(new YamlError(new Error(`Event definitions must be objects.`)));
        continue;
      }

      const payloads = value.get('payloads') as YAMLSeq;
      if (false == payloads instanceof YAMLSeq) {
        this.#errors.push(new YamlError(new Error(`Event definition payloads must be an array.`)));
        continue;
      }

      const eventDescriptor = new SqlEventDescriptor(key.toString(), compatibility);
      for (let item of payloads.items) {
        if (!isScalar(item)) {
          this.#errors.push(new YamlError(new Error(`Payload queries for events must be scalar.`)));
          continue;
        }
        this.#withScalar(item, (q) => {
          return eventDescriptor.addSourceQuery(q, this.options);
        });
      }

      eventDescriptors.push(eventDescriptor);
    }

    return eventDescriptors;
  }

  #checkUniqueName(name: string, literal: Scalar): boolean {
    if (this.#definitionNames.has(name)) {
      this.#errors.push(this.#tokenError(literal, 'Duplicate stream or bucket definition.'));
      return false;
    }

    this.#definitionNames.add(name);
    return true;
  }

  #parsePriority(value: YAMLMap) {
    if (value.has('priority')) {
      const priorityValue = value.get('priority', true)!;
      if (typeof priorityValue.value != 'number' || !isValidPriority(priorityValue.value)) {
        this.#errors.push(
          this.#tokenError(priorityValue, 'Invalid priority, expected a number between 0 and 3 (inclusive).')
        );
      } else {
        return priorityValue.value;
      }
    }
  }

  #acceptPotentiallyUnsafeQueries(definition: YAMLMap): boolean {
    return definition.get('accept_potentially_dangerous_queries', true)?.value == true;
  }

  #yamlError(node: Node, message: string) {
    if (node.range != null) {
      const [start, _, end] = node.range;
      return new YamlError(new Error(message), { start, end });
    } else {
      return new YamlError(new Error(message));
    }
  }

  #tokenError(token: Scalar, message: string) {
    const start = token?.srcToken?.offset ?? 0;
    const end = start + 1;
    return new YamlError(new Error(message), { start, end });
  }

  #withScalar(scalar: Scalar, cb: (value: string) => QueryParseResult) {
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
      this.#addErrorFromScalar(scalar, value, err);
    }
    return result;
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
    // For block scalars (| and >), srcToken.offset points to the header character; skip past
    // the header line to reach the first line of actual content.
    const tokenStart = isBlockScalar(scalar.type) ? this.yaml.indexOf('\n', srcToken.offset) + 1 : srcToken.offset;
    // For quoted scalars, the opening quote is part of the token but not the parsed value.
    // tokenStart and valueStart are only different for quoted scalars.
    const valueStart = isQuotedScalar(scalar.type) ? tokenStart + 1 : tokenStart;

    let offset: number;
    let end: number;

    if (err instanceof SqlRuleError && err.location) {
      // Use an offset map to translate parsed-value positions to source positions, handling
      // escape sequences in quoted scalars and stripped indentation in block scalars.
      // Slice from tokenStart so block scalars don't include the header line (| or >) in the map.
      const tokenSource = this.yaml.slice(tokenStart, scalar.range![1]);
      const offsetMap = buildParsedToSourceValueMap(tokenSource, scalar.type);
      offset = tokenStart + (offsetMap[err.location.start] ?? err.location.start);
      end = tokenStart + (offsetMap[err.location.end] ?? err.location.end);
    } else if (typeof (err as any).token?._location?.start == 'number') {
      offset = valueStart + (err as any).token?._location?.start;
      end = valueStart + (err as any).token?._location?.end;
    } else {
      offset = valueStart;
      end = valueStart + Math.max(value.length, 1);
    }

    const pos = { start: offset, end };
    this.#errors.push(new YamlError(err, pos));
  }
}

function isBlockScalar(type: string | null | undefined): boolean {
  return type === Scalar.BLOCK_LITERAL || type === Scalar.BLOCK_FOLDED;
}

function isQuotedScalar(type: string | null | undefined): boolean {
  return type === Scalar.QUOTE_DOUBLE || type === Scalar.QUOTE_SINGLE;
}

/**
 * Builds a map from indexes in parsed YAML scalars to indexes in the source YAML file.
 *
 * Quoted scalars (", ') have escape sequences that take up more chars in the source relative to the parsed value.
 * Multi-line scalars (|, >) have line folding where newline + surrounding whitespace collapses
 * to a single space. In both cases value offsets don't map 1:1 to source offsets.
 *
 * @param src The raw YAML source of the scalar token.
 * @param scalarType The Scalar.type value.
 */
function buildParsedToSourceValueMap(src: string, scalarType: string | null | undefined): number[] {
  const map: number[] = [];

  if (isQuotedScalar(scalarType)) {
    const isDouble = scalarType === Scalar.QUOTE_DOUBLE;

    let srcIdx = 1; // Skip opening quote
    let valIdx = 0;
    const eof = src.length - 1; // Skip closing quote
    while (srcIdx < eof) {
      map[valIdx] = srcIdx;
      const current = src[srcIdx];
      const next = src[srcIdx + 1];

      if (isDouble && current === '\\') {
        // Handle escape chars / unicode
        if (next === 'x')
          srcIdx += 4; // \xXX
        else if (next === 'u')
          srcIdx += 6; // \uXXXX
        else if (next === 'U')
          srcIdx += 10; // \UXXXXXXXX
        else srcIdx += 2; // \n, \", \\, etc.
      } else if (!isDouble && current === "'" && next === "'") {
        // Handle '' in single quotes
        srcIdx += 2;
      } else {
        srcIdx++;
      }
      valIdx++;
    }
    map[valIdx] = srcIdx;
  } else if (isBlockScalar(scalarType)) {
    // Detect indentation using first non-empty line
    let trimIndent = 0;
    let i = 0;
    outer: while (i < src.length) {
      // Skip to end of line
      const lineStart = i;
      while (i < src.length && src[i] !== '\n') i++;
      // Count chars from start of line until first non-whitespace char
      for (let j = lineStart; j < i; j++) {
        if (src[j] !== ' ' && src[j] !== '\t') {
          trimIndent = j - lineStart;
          break outer;
        }
      }
      // No non-whitespace char found; go to next line
      i++;
    }

    // Skip trimIndent chars for each line
    let valIdx = 0;
    let srcIdx = 0;
    while (srcIdx < src.length) {
      let lineEnd = srcIdx;
      while (lineEnd < src.length && src[lineEnd] !== '\n') lineEnd++;
      const lineLen = lineEnd - srcIdx;
      // Prevent skipping into next line for zero-length or very short lines
      const contentStart = srcIdx + Math.min(trimIndent, lineLen);
      for (let p = contentStart; p <= lineEnd && p < src.length; p++) {
        map[valIdx++] = p;
      }
      srcIdx = lineEnd + 1;
    }
    map[valIdx] = src.length; // Sentinel
  } else if (scalarType === Scalar.PLAIN) {
    // Plain scalars may span multiple lines. Line folding collapses each
    // (trailing whitespace + newline + leading whitespace of next line) into a single space.
    let valIdx = 0;
    let srcIdx = 0;
    let firstLine = true;
    while (srcIdx <= src.length) {
      // Find content bounds for this line
      let lineEnd = srcIdx;
      while (lineEnd < src.length && src[lineEnd] !== '\n' && src[lineEnd] !== '\r') lineEnd++;

      let contentStart = srcIdx;
      if (!firstLine) {
        // Strip leading whitespace from continuation lines
        while (contentStart < lineEnd && (src[contentStart] === ' ' || src[contentStart] === '\t')) contentStart++;
      }
      let contentEnd = lineEnd;
      while (contentEnd > contentStart && (src[contentEnd - 1] === ' ' || src[contentEnd - 1] === '\t')) contentEnd--;

      if (!firstLine) {
        // The folded newline becomes a single space; map it to the start of this line's content
        map[valIdx++] = contentStart;
      }
      for (let p = contentStart; p < contentEnd; p++) {
        map[valIdx++] = p;
      }

      if (lineEnd >= src.length) break;
      srcIdx = lineEnd + 1;
      if (src[lineEnd] === '\r' && src[srcIdx] === '\n') srcIdx++; // \r\n counts as one newline
      firstLine = false;
    }
    map[valIdx] = src.length; // Sentinel
  }

  return map;
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
