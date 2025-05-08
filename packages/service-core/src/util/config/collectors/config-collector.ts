import * as yaml from 'yaml';

import { schema } from '@powersync/lib-services-framework';
import { configFile } from '@powersync/service-types';

import { RunnerConfig } from '../types.js';
import { YamlEnvTag } from './impl/yaml-env.js';

export enum ConfigFileFormat {
  YAML = 'yaml',
  JSON = 'json'
}

// ts-codec itself doesn't give great validation errors, so we use json schema for that
const configSchemaValidator = schema.parseJSONSchema(configFile.PowerSyncConfigJSONSchema).validator();

export abstract class ConfigCollector {
  abstract get name(): string;

  /**
   * Collects the serialized base PowerSyncConfig.
   * @returns null if this collector cannot provide a config
   */
  abstract collectSerialized(runnerConfig: RunnerConfig): Promise<configFile.SerializedPowerSyncConfig | null>;

  /**
   * Collects the PowerSyncConfig settings.
   * Validates and decodes the config.
   * @returns null if this collector cannot provide a config
   */
  async collect(runner_config: RunnerConfig): Promise<configFile.PowerSyncConfig | null> {
    const serialized = await this.collectSerialized(runner_config);
    if (!serialized) {
      return null;
    }

    /**
     * After this point a serialized config has been found. Any failures to decode or validate
     * will result in a hard stop.
     */
    const decoded = this.decode(serialized);
    this.validate(decoded);
    return decoded;
  }

  /**
   * Validates input config
   * ts-codec itself doesn't give great validation errors, so we use json schema for that
   */
  validate(config: configFile.PowerSyncConfig) {
    const valid = configSchemaValidator.validate(config);
    if (!valid.valid) {
      throw new Error(`Failed to validate PowerSync config: ${valid.errors.join(', ')}`);
    }
  }

  decode(encoded: configFile.SerializedPowerSyncConfig): configFile.PowerSyncConfig {
    try {
      return configFile.powerSyncConfig.decode(encoded);
    } catch (ex) {
      throw new Error(`Failed to decode PowerSync config: ${ex}`);
    }
  }

  protected parseContent(content: string, contentType?: ConfigFileFormat) {
    switch (contentType) {
      case ConfigFileFormat.YAML:
        return this.parseYaml(content);
      case ConfigFileFormat.JSON:
        return this.parseJSON(content);
      default: {
        // No content type provided, need to try both
        try {
          return this.parseYaml(content);
        } catch (ex) {}
        try {
          return this.parseJSON(content);
        } catch (ex) {
          throw new Error(`Could not parse PowerSync config file content as JSON or YAML: ${ex}`);
        }
      }
    }
  }

  protected parseYaml(content: string) {
    const lineCounter = new yaml.LineCounter();

    const parsed = yaml.parseDocument(content, {
      schema: 'core',
      keepSourceTokens: true,
      lineCounter,
      customTags: [YamlEnvTag]
    });

    if (parsed.errors.length) {
      throw new Error(
        `Could not parse YAML configuration file. Received errors: \n ${parsed.errors.map((e) => e.message).join('\n')}`
      );
    }

    return parsed.toJS();
  }

  protected parseJSON(content: string) {
    return JSON.parse(content);
  }
}
