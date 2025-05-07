import { AuthorizationError } from '@powersync/lib-services-framework';
import { KeySpec } from './KeySpec.js';

export interface KeyCollector {
  /**
   * Fetch keys for this collector.
   *
   * If a partial result is available, return keys and errors array.
   * These errors are not retried, and previous keys not cached.
   *
   * If the request fails completely, throw an error. These errors are retried.
   * In that case, previous keys may be cached and used.
   */
  getKeys(): Promise<KeyResult>;

  /**
   * Indicates that no matching key was found.
   *
   * The collector may use this as a hint to reload keys, although this is not a requirement.
   */
  noKeyFound?: () => Promise<void>;
}

export interface KeyResult {
  errors: AuthorizationError[];
  keys: KeySpec[];
}
