import { SyncRulesConfig } from '../types.js';
import fs from 'fs/promises';

export interface SyncRulesProvider {
  get(): Promise<string | undefined>;
}

export class ConfigurationFileSyncRulesProvider implements SyncRulesProvider {
  constructor(private config: SyncRulesConfig) {}

  async get(): Promise<string | undefined> {
    if (this.config.content) {
      return this.config.content;
    } else if (this.config.path) {
      return await fs.readFile(this.config.path, 'utf-8');
    }
  }
}
