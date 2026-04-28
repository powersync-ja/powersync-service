import * as jose from 'jose';
import { KeyCollector, KeyResult } from './KeyCollector.js';
import { KeySpec } from './KeySpec.js';

/**
 * Set of static keys.
 *
 * A key can be added both with and without a kid, in case wildcard matching is desired.
 */
export class StaticKeyCollector implements KeyCollector {
  static async importKeys(keys: jose.JWK[]) {
    const parsedKeys = await Promise.all(keys.map((key) => KeySpec.importKey(key)));
    return new StaticKeyCollector(parsedKeys);
  }

  constructor(private keys: KeySpec[]) {}

  async getKeys(): Promise<KeyResult> {
    return { keys: this.keys, errors: [] };
  }
}
