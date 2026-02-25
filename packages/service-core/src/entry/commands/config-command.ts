import { Command } from 'commander';

import * as util from '../../util/util-index.js';

/**
 * Wraps a Command with the standard config options
 */
export function wrapConfigCommand(command: Command) {
  return command
    .option(
      `-c, --config-path [path]`,
      'Path (inside container) to YAML config file. Defaults to process.env.POWERSYNC_CONFIG_PATH',
      util.env.POWERSYNC_CONFIG_PATH
    )
    .option(
      `-c64, --config-base64 [base64]`,
      'Base64 encoded YAML or JSON config file. Defaults to process.env.POWERSYNC_CONFIG_B64',
      util.env.POWERSYNC_CONFIG_B64
    )
    .option(
      `-sync64, --sync-base64 [base64]`,
      'Base64 encoded YAML Sync Config. Defaults to process.env.POWERSYNC_SYNC_CONFIG_B64 or process.env.POWERSYNC_SYNC_RULES_B64 for backwards compatility.',
      util.env.POWERSYNC_SYNC_CONFIG_B64 || util.env.POWERSYNC_SYNC_RULES_B64
    );
}

/**
 * Extracts runner configuration params from Command options.
 */
export function extractRunnerOptions(options: any): util.RunnerConfig {
  return {
    config_path: options.configPath,
    config_base64: options.configBase64,
    sync_config_base64: options.syncBase64
  };
}
