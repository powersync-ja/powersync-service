import * as jose from 'jose';
import { KeySpec } from './KeySpec.js';
import { KeyCollector, KeyResult } from './KeyCollector.js';

export class CompoundKeyCollector implements KeyCollector {
  private collectors: KeyCollector[];

  constructor(collectors?: KeyCollector[]) {
    this.collectors = collectors ?? [];
  }

  add(collector: KeyCollector) {
    this.collectors.push(collector);
  }

  async getKeys(): Promise<KeyResult> {
    let keys: KeySpec[] = [];
    let errors: jose.errors.JOSEError[] = [];
    const promises = this.collectors.map((collector) =>
      collector.getKeys().then((result) => {
        keys.push(...result.keys);
        errors.push(...result.errors);
      })
    );
    await Promise.all(promises);
    return { keys, errors };
  }

  async noKeyFound(): Promise<void> {
    const promises = this.collectors.map((collector) => collector.noKeyFound?.());
    await Promise.all(promises);
  }
}
