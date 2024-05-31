import { ConfigCollector } from '../config-collector.js';
import { RunnerConfig } from '../../types.js';

export class Base64ConfigCollector extends ConfigCollector {
  get name(): string {
    return 'Base64';
  }

  async collectSerialized(runnerConfig: RunnerConfig) {
    const { config_base64 } = runnerConfig;
    if (!config_base64) {
      return null;
    }

    // Could be JSON or YAML at this point
    return this.parseContent(Buffer.from(config_base64, 'base64').toString());
  }
}
