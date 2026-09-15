import {
  AdditionalSyncConfigParser,
  compileSyncRulesSchemaValidator,
  createSyncRulesSchema,
  JsonObject,
  normalizeConnectionConfig,
  SqlSyncRules,
  SyncConfig,
  SyncConfigDiagnostic,
  SyncConfigWithErrors,
  SyncRulesErrors,
  SyncRulesOptions,
  validateAdditionalSyncConfigParsers,
  YamlError
} from '@powersync/service-sync-rules';

export type ParseSyncConfigYamlOptions = Omit<SyncRulesOptions, 'parsers' | 'jsonSchema'>;

/** Shared by service routes, deployment, replication and persisted storage; only initialization can register hooks. */
export interface SyncConfigParser {
  readonly jsonSchema: JsonObject;
  parseYaml(content: string, options: ParseSyncConfigYamlOptions): SyncConfigWithErrors;
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

  /** Validate the candidate composition before mutating the registry, so a failed registration is atomic. */
  registerParser(parser: AdditionalSyncConfigParser): void {
    const parsers = [...this.#parsers, parser];
    validateAdditionalSyncConfigParsers(parsers);
    const schema = createSyncRulesSchema(parsers);
    compileSyncRulesSchemaValidator(schema);
    this.#parsers.push(parser);
    this.#jsonSchema = schema;
  }

  parseYaml(content: string, options: ParseSyncConfigYamlOptions): SyncConfigWithErrors {
    return SqlSyncRules.fromYaml(content, { ...options, parsers: this.#parsers, jsonSchema: this.#jsonSchema });
  }

  validatePersisted({ config, context }: { config: SyncConfig; context: { defaultSchema: string } }): YamlError[] {
    // Validate the saved field with the same composed definitions as authoring. Removing a module removes acceptance
    // of its table options, even though no module hook remains available to notice them.
    const configSchema = (this.#jsonSchema.properties as JsonObject).config as JsonObject;
    const validate = compileSyncRulesSchemaValidator({
      type: 'object',
      definitions: this.#jsonSchema.definitions,
      $defs: this.#jsonSchema.$defs,
      required: ['config'],
      properties: {
        config: {
          type: 'object',
          required: ['connections'],
          properties: {
            connections: (configSchema.properties as JsonObject).connections
          },
          additionalProperties: false
        }
      },
      additionalProperties: false
    });
    const errors: YamlError[] = [];
    const reportDiagnostic = (diagnostic: SyncConfigDiagnostic) => {
      const error = new YamlError(new Error(diagnostic.message));
      error.type = diagnostic.level;
      errors.push(error);
    };
    if (!validate({ config: { connections: config.connectionConfig } })) {
      throw new Error(
        `Unsupported persisted connection configuration: ${validate.errors!.map((e: any) => e.message).join(', ')}`
      );
    }
    config.connectionConfig = normalizeConnectionConfig(config.connectionConfig);
    for (const parser of this.#parsers) {
      parser.validatePersisted?.({ config, context: { ...context, reportDiagnostic } });
    }
    if (errors.some((error) => error.type == 'fatal')) throw new SyncRulesErrors(errors);
    return errors;
  }
}
