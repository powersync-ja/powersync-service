import { utils } from '@powersync/lib-services-framework';

import { ServiceRunner } from './config/types.js';

export const env = utils.collectEnvironmentVariables({
  /**
   * Path to configuration file in filesystem
   */
  POWERSYNC_CONFIG_PATH: utils.type.string.optional(),
  /**
   * Base64 encoded contents of configuration file
   */
  POWERSYNC_CONFIG_B64: utils.type.string.optional(),
  /**
   * Base64 encoded contents of sync rules YAML
   */
  POWERSYNC_SYNC_RULES_B64: utils.type.string.optional(),
  /**
   * Runner to be started in this process
   */
  PS_RUNNER_TYPE: utils.type.string.default(ServiceRunner.UNIFIED),
  /**
   * Port for metrics
   */
  METRICS_PORT: utils.type.number.optional(),

  NODE_ENV: utils.type.string.optional()
});

export type Env = typeof env;
