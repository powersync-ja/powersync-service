import type { JsonObject } from './json.js';
import type { SyncConfig } from './SyncConfig.js';
import type { TablePattern } from './TablePattern.js';

/** A decoded YAML path, for example `['config', 'connections', 'default', 'tables', 'orders', 'filter']`. */
export type SyncConfigSourcePath = readonly (string | number)[];
export type SyncConfigSourceLocationTarget = 'key' | 'value';

/** Offsets into the authored YAML, compatible with locations returned by service diagnostics. */
export interface SyncConfigSourceSpan {
  start_offset: number;
  end_offset: number;
}

/** A parser warning or failure, optionally pointing to the relevant YAML key or value. */
export interface SyncConfigDiagnostic {
  level: 'warning' | 'fatal';
  message: string;
  location?: SyncConfigSourceSpan;
}

export interface SyncConfigSourceLocationResolver {
  /** Resolve a field to its YAML span, falling back to its nearest existing ancestor. */
  getLocation(path: SyncConfigSourcePath, target?: SyncConfigSourceLocationTarget): SyncConfigSourceSpan | undefined;
}

export interface SyncConfigParserContext {
  /** Candidate being assembled. It is not published if any parser fails. Store options on the appropriate config fields. */
  parsedConfig: SyncConfig;
  /** Sources selected by SQL. Additional parsers may inspect these, but cannot add replication sources. */
  sourceTables: readonly TablePattern[];
  defaultSchema: string;
  sourceLocations: SyncConfigSourceLocationResolver;
  reportDiagnostic(diagnostic: SyncConfigDiagnostic): void;
}

export interface PersistedSyncConfigParserContext {
  defaultSchema: string;
  reportDiagnostic(diagnostic: SyncConfigDiagnostic): void;
}

/**
 * Additional deterministic parsing/validation, registered per service rather than globally or by connection type.
 * A hook identifies its own fields and leaves unrelated input alone. Source I/O belongs in source validation.
 */
export interface AdditionalSyncConfigParser {
  readonly id: string;
  /** Mutates an isolated schema before parsing; this hook is not restricted to connection configuration. */
  extendJsonSchema?(options: { schema: JsonObject }): void;
  /** `config` is decoded YAML. Everything else needed to parse it is supplied through `context`. */
  parse(options: { config: unknown; context: SyncConfigParserContext }): void;
  /** Validate saved config fields without reparsing their original YAML or requiring YAML source locations. */
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
