import * as fs from 'fs/promises';
import * as micro from '@journeyapps-platform/micro';

import { ConfigCollector, ConfigFileFormat } from '../config-collector.js';
import { RunnerConfig } from '../../types.js';

export class FileSystemConfigCollector extends ConfigCollector {
  get name(): string {
    return 'FileSystem';
  }

  async collectSerialized(runnerConfig: RunnerConfig) {
    const { config_path } = runnerConfig;
    if (!config_path) {
      return null;
    }

    // Check if file exists
    try {
      await fs.access(config_path, fs.constants.F_OK);
    } catch (ex) {
      throw new Error(`Config file path ${config_path} was specified, but the file does not exist.`);
    }

    micro.logger.info(`Collecting PowerSync configuration from File: ${config_path}`);

    const content = await fs.readFile(config_path, 'utf-8');

    let contentType: ConfigFileFormat | undefined;
    switch (true) {
      case config_path.endsWith('.yaml'):
      case config_path.endsWith('.yml'):
        contentType = ConfigFileFormat.YAML;
        break;
      case config_path.endsWith('.json'):
        contentType = ConfigFileFormat.JSON;
        break;
    }
    return this.parseContent(content, contentType);
  }
}
