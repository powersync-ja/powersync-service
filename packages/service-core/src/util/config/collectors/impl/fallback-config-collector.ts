import { RunnerConfig } from '../../types.js';
import { FileSystemConfigCollector } from './filesystem-config-collector.js';

export const DEFAULT_CONFIG_LOCATION = 'powersync.yaml';

/**
 * Falls back to reading the PowerSync config file from the previous
 * default location of `powersync.yaml`
 */
export class FallbackConfigCollector extends FileSystemConfigCollector {
  get name(): string {
    return 'Fallback powersync.yaml';
  }

  async collectSerialized(runnerConfig: RunnerConfig) {
    return super.collectSerialized({
      ...runnerConfig,
      // Use the fallback config location
      config_path: DEFAULT_CONFIG_LOCATION
    });
  }
}
