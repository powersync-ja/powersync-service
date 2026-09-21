import {
  AdditionalSyncConfigParser,
  compileSyncRulesSchemaValidator,
  createSyncRulesSchema,
  JsonObject,
  SqlSyncRules,
  SyncConfig,
  SyncConfigDiagnostic,
  SyncConfigWithErrors,
  SyncRulesErrors,
  SyncRulesOptions,
  validateAdditionalSyncConfigParsers,
  YamlError
} from '@powersync/service-sync-rules';

export type ParseSyncConfigOptions = Omit<SyncRulesOptions, 'parsers' | 'jsonSchema'>;

/**
 * Shared by service routes, deployment, replication and persisted storage; only initialization can register hooks.
 */
export interface SyncConfigParser {
  /**
   * A JSON schema which can be used to validate the format of a supplied sync config.
   */
  readonly jsonSchema: JsonObject;

  /**
   * Parses string content.
   *
   * @returns The parsed {@link SyncConfigWithErrors}
   */
  parseContent(content: string, options: ParseSyncConfigOptions): SyncConfigWithErrors;

  /**
   * Validates a persisted/parsed sync config.
   *
   * @returns Any errors detected.
   */
  validatePersisted(options: { config: SyncConfig; context: { defaultSchema: string } }): YamlError[];
}

export class SqlSyncConfigParser implements SyncConfigParser {
  readonly #parsers: AdditionalSyncConfigParser[];
  #jsonSchema: JsonObject;

  constructor(parsers: readonly AdditionalSyncConfigParser[] = []) {
    validateAdditionalSyncConfigParsers(parsers);
    this.#parsers = [...parsers];
    this.#jsonSchema = createSyncRulesSchema(parsers);
    compileSyncRulesSchemaValidator(this.#jsonSchema);
  }

  get jsonSchema(): JsonObject {
    // Tooling can customize its copy without changing validation for the running service.
    return structuredClone(this.#jsonSchema);
  }

  /**
   * Validate the candidate composition before mutating the registry.
   */
  registerParser(parser: AdditionalSyncConfigParser): void {
    const parsers = [...this.#parsers, parser];
    validateAdditionalSyncConfigParsers(parsers);
    const schema = createSyncRulesSchema(parsers);
    compileSyncRulesSchemaValidator(schema);
    this.#parsers.push(parser);
    this.#jsonSchema = schema;
  }

  parseContent(content: string, options: ParseSyncConfigOptions): SyncConfigWithErrors {
    return SqlSyncRules.fromYaml(content, { ...options, parsers: this.#parsers, jsonSchema: this.#jsonSchema });
  }

  validatePersisted({ config, context }: { config: SyncConfig; context: { defaultSchema: string } }): YamlError[] {
    /**
     * Ensure that all modules which created a persisted SyncConfig
     * are currently loaded.
     * This prevents a case for a SyncConfig with missing external modules to be loaded.
     */
    const registeredIds = new Set(this.#parsers.map((parser) => parser.id));
    const missingIds = [...config.additionalModuleIds].filter((id) => !registeredIds.has(id));
    if (missingIds.length != 0) {
      throw new Error(`Missing required sync config parsers: ${missingIds.join(', ')}`);
    }

    const errors: YamlError[] = [];
    const reportDiagnostic = (diagnostic: SyncConfigDiagnostic) => {
      const location = diagnostic.location;
      const error = new YamlError(
        new Error(diagnostic.message),
        location && {
          start: location.start_offset,
          end: location.end_offset
        }
      );
      error.type = diagnostic.level;
      errors.push(error);
    };

    for (const parser of this.#parsers) {
      if (!config.additionalModuleIds.has(parser.id)) continue;
      /**
       * This allows additional validation of module specific semantics
       */
      parser.validatePersisted?.({ config, context: { ...context, reportDiagnostic } });
    }
    if (errors.some((error) => error.type == 'fatal')) throw new SyncRulesErrors(errors);
    return errors;
  }
}
