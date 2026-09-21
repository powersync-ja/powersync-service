import type { JsonObject } from './json.js';
import type { SyncConfig } from './SyncConfig.js';
import type { TablePattern } from './TablePattern.js';

/**
 * A decoded config path, for example `['config', 'connections', 'default', 'tables', 'orders', 'filter']`.
 */
export type SyncConfigSourcePath = readonly (string | number)[];
export type SyncConfigSourceLocationTarget = 'key' | 'value';

/**
 * Offsets into the authored config, compatible with locations returned by service diagnostics.
 */
export interface SyncConfigSourceSpan {
  start_offset: number;
  end_offset: number;
}

/**
 * A parser warning or failure, optionally pointing to the relevant config key or value.
 */
export interface SyncConfigDiagnostic {
  level: 'warning' | 'fatal';
  message: string;
  location?: SyncConfigSourceSpan;
}

export interface SyncConfigSourceLocationResolver {
  /**
   * Resolve a field to its source span, falling back to its nearest existing ancestor.
   */
  getLocation(path: SyncConfigSourcePath, target?: SyncConfigSourceLocationTarget): SyncConfigSourceSpan | undefined;
}

/**
 * Context for parsing input sync config.
 */
export interface SyncConfigParserContext {
  readonly defaultSchema: string;
  /**
   * Candidate being assembled. It is not published if any parser fails. Store options on the appropriate config fields.
   */
  parsedConfig: SyncConfig;
  /**
   * Sources selected by SQL. Additional parsers may inspect these, but cannot add replication sources.
   */
  sourceTables: readonly TablePattern[];
  /**
   * Utility helpers for determining source locations.
   */
  sourceLocations: SyncConfigSourceLocationResolver;
  /**
   * Report a diagnostic found while parsing.
   */
  reportDiagnostic(diagnostic: SyncConfigDiagnostic): void;
}

/**
 * Context for validating persisted sync config.
 */
export interface PersistedSyncConfigParserContext {
  /**
   * Default schema used to resolve unqualified table names during validation.
   * Some callers supply a placeholder; do not persist names qualified using this value.
   */
  readonly defaultSchema: string;
  /**
   * Report a warning or fatal error found while validating the persisted config.
   * Include a config source location when available. Fatal errors prevent the config from loading.
   */
  reportDiagnostic(diagnostic: SyncConfigDiagnostic): void;
}

/**
 * Additional deterministic parsing/validation, registered per service rather than globally or by connection type.
 * A hook identifies its own fields and leaves unrelated input alone. Source I/O belongs in source validation.
 */
export interface AdditionalSyncConfigParser {
  /**
   * A unique ID for the parser. Persisted sync configs store these IDs to validate
   * if modules which created the persisted config are present.
   */
  readonly id: string;

  /**
   * Mutates an isolated schema before parsing.
   */
  extendJsonSchema?(options: { schema: JsonObject }): void;

  /**
   * `config` is decoded sync config. Everything else needed to parse it is supplied through `context`.
   * Parse module-owned input into context.parsedConfig, including connectionConfig entries for this module.
   * Core does not copy connection options from the input or normalize the parser's output here.
   * For a PrecompiledSyncConfig, add this parser's ID to parsedConfig.plan.moduleData with a null value
   * when the config requires this module. The plan's keys determine which parsers must be present on reload.
   */
  parse(options: { config: unknown; context: SyncConfigParserContext }): void;

  /**
   * Validate saved config fields without reparsing their original config or requiring config source locations.
   */
  validatePersisted?(options: { config: SyncConfig; context: PersistedSyncConfigParserContext }): void;
}

export function validateAdditionalSyncConfigParsers(parsers: readonly AdditionalSyncConfigParser[]): void {
  const ids = new Set<string>();
  for (const parser of parsers) {
    if (!parser.id) throw new Error('A sync config parser id must not be empty.');
    if (ids.has(parser.id)) throw new Error(`A sync config parser is already registered with id '${parser.id}'.`);
    ids.add(parser.id);
  }
}
