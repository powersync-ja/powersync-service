import * as jose from 'jose';
import { KeySpec, KeyOptions } from './KeySpec.js';
import { KeyCollector, KeyResult } from './KeyCollector.js';

const SUPABASE_KEY_OPTIONS: KeyOptions = {
  requiresAudience: ['authenticated'],
  maxLifetimeSeconds: 86400 * 7 + 1200 // 1 week + 20 minutes margin
};

/**
 * Set of static keys for Supabase.
 *
 * Same as StaticKeyCollector, but with some configuration tweaks for Supabase.
 *
 * Similar to SupabaseKeyCollector, but using hardcoded keys instead of fetching
 * from the database.
 *
 * A key can be added both with and without a kid, in case wildcard matching is desired.
 */
export class StaticSupabaseKeyCollector implements KeyCollector {
  static async importKeys(keys: jose.JWK[]) {
    const parsedKeys = await Promise.all(keys.map((key) => KeySpec.importKey(key, SUPABASE_KEY_OPTIONS)));
    return new StaticSupabaseKeyCollector(parsedKeys);
  }

  constructor(private keys: KeySpec[]) {}

  async getKeys(): Promise<KeyResult> {
    return { keys: this.keys, errors: [] };
  }
}
