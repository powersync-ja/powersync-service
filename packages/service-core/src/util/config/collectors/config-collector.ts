import * as yaml from 'yaml';

import { configFile } from '@powersync/service-types';

import { RunnerConfig } from '../types.js';

export enum ConfigFileFormat {
  YAML = 'yaml',
  JSON = 'json'
}

/**
 * Environment variables can be substituted into the YAML config
 * when parsing if the environment variable name starts with this prefix.
 * Attempting to substitute any other environment variable will throw an exception.
 *
 * Example of substitution:
 * storage:
 *    type: mongodb
 *    uri: !env PS_MONGO_URI
 */
const YAML_ENV_PREFIX = 'PS_';

export abstract class ConfigCollector {
  abstract get name(): string;

  /**
   * Collects the serialized base PowerSyncConfig.
   * @returns null if this collector cannot provide a config
   */
  abstract collectSerialized(runnerConfig: RunnerConfig): Promise<configFile.SerializedPowerSyncConfig | null>;

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
      customTags: [
        {
          tag: '!env',
          resolve(envName: string, onError: (error: string) => void) {
            if (!envName.startsWith(YAML_ENV_PREFIX)) {
              onError(
                `Attempting to substitute environment variable ${envName} is not allowed. Variables must start with "${YAML_ENV_PREFIX}"`
              );
              return envName;
            }
            const value = process.env[envName];
            if (typeof value == 'undefined') {
              onError(
                `Attempted to substitute environment variable "${envName}" which is undefined. Set this variable on the environment.`
              );
              return envName;
            }
            return value;
          }
        }
      ]
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
