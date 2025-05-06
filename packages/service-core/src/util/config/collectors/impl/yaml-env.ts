import * as yaml from 'yaml';

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

/**
 * Custom YAML tag which performs string environment variable substitution
 */
export const YamlEnvTag: yaml.ScalarTag = {
  tag: '!env',
  resolve(envName: string, onError: (error: string) => void): string {
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
};

/**
 * Casts the environment variable to a boolean.
 */
export const YamlEnvTagBoolean: yaml.ScalarTag = {
  tag: '!env_boolean',
  resolve(envName: string, onError: (error: string) => void) {
    const stringValue = YamlEnvTag.resolve(envName, onError, {});
    if (stringValue == envName) {
      // This is returned for error conditions
      return stringValue;
    }
    if (typeof stringValue !== 'string') {
      // should not reach this point
      onError(`Environment variable "${envName}" is not a string.`);
      return stringValue;
    }
    if (stringValue.toLowerCase() == 'true') {
      return true;
    } else if (stringValue.toLowerCase() == 'false') {
      return false;
    } else {
      onError(`Environment variable "${envName}" is not a boolean. Expected "true" or "false", got "${stringValue}".`);
      return stringValue;
    }
  }
};

/**
 * Casts the environment variable to a number.
 */
export const YamlEnvTagNumber: yaml.ScalarTag = {
  tag: '!env_number',
  resolve(envName: string, onError: (error: string) => void) {
    const stringValue = YamlEnvTag.resolve(envName, onError, {});
    if (stringValue == envName) {
      // This is returned for error conditions from YamlEnvTag
      return stringValue;
    }

    if (typeof stringValue !== 'string') {
      onError(`Environment variable "${envName}" is not a string.`);
      return stringValue;
    }

    const numberValue = Number(stringValue);
    if (Number.isNaN(numberValue)) {
      onError(`Environment variable "${envName}" is not a valid number. Got: "${stringValue}".`);
      return stringValue;
    }

    return numberValue;
  }
};
