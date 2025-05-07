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
 * Allows for type casting string environment variables to boolean or number
 * by using the syntax !env PS_MONGO_PORT::number or !env PS_USE_SUPABASE::boolean
 */
export const YamlEnvTag: yaml.ScalarTag = {
  tag: '!env',
  resolve(envName: string, onError: (error: string) => void) {
    if (!envName.startsWith(YAML_ENV_PREFIX)) {
      onError(
        `Attempting to substitute environment variable ${envName} is not allowed. Variables must start with "${YAML_ENV_PREFIX}"`
      );
      return envName;
    }

    // allow type casting if the envName contains a type suffix
    // e.g. PS_MONGO_PORT::number or PS_USE_SUPABASE::boolean
    const [name, type = 'string'] = envName.split('::');

    let value = process.env[name];

    if (typeof value == 'undefined') {
      onError(
        `Attempted to substitute environment variable "${envName}" which is undefined. Set this variable on the environment.`
      );
      return envName;
    }

    switch (type) {
      case 'string':
        return value;
      case 'number':
        const numberValue = Number(value);
        if (Number.isNaN(numberValue)) {
          onError(`Environment variable "${envName}" is not a valid number. Got: "${value}".`);
          return envName;
        }
        return numberValue;
      case 'boolean':
        if (value?.toLowerCase() == 'true') {
          return true;
        } else if (value?.toLowerCase() == 'false') {
          return false;
        } else {
          onError(`Environment variable "${envName}" is not a boolean. Expected "true" or "false", got "${value}".`);
          return envName;
        }
      default:
        onError(`Environment variable "${envName}" has an invalid type suffix "${type}".`);
        return envName;
    }
  }
};
