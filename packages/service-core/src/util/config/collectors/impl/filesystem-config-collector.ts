import * as fs from 'fs/promises';
import * as path from 'path';

import { ConfigCollector, ConfigFileFormat } from '../config-collector.js';
import { RunnerConfig } from '../../types.js';
import { logger } from '../../../../system/Logger.js';

export class FileSystemConfigCollector extends ConfigCollector {
  get name(): string {
    return 'FileSystem';
  }

  async collectSerialized(runnerConfig: RunnerConfig) {
    const { config_path } = runnerConfig;
    if (!config_path) {
      return null;
    }

    const resolvedPath = path.resolve(process.cwd(), config_path);

    // Check if file exists
    try {
      await fs.access(resolvedPath, fs.constants.F_OK);
    } catch (ex) {
      throw new Error(`Config file path ${resolvedPath} was specified, but the file does not exist.`);
    }

    logger.info(`Collecting PowerSync configuration from File: ${resolvedPath}`);
    const content = await fs.readFile(resolvedPath, 'utf-8');

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
