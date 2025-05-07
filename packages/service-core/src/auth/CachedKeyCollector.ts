import * as jose from 'jose';
import timers from 'timers/promises';
import { KeySpec } from './KeySpec.js';
import { LeakyBucket } from './LeakyBucket.js';
import { KeyCollector, KeyResult } from './KeyCollector.js';
import { AuthorizationError2 } from '@powersync/lib-services-framework';
import { mapAuthConfigError } from './utils.js';

/**
 * Manages caching and refreshing for a key collector.
 *
 * Cache refreshing is activity-based, instead of automatically refreshing.
 *
 * Generally:
 *  * If the last refresh was > 5 minutes ago, trigger a background refresh, but use the cached keys.
 *  * If the last refresh was > 60 minutes ago, discard the cache, and force a refresh.
 *  * If the last refresh resulted in an error, refresh based on a retry delay, but use cached keys.
 */

export class CachedKeyCollector implements KeyCollector {
  private currentKeys: KeySpec[] = [];
  /**
   * The time that currentKeys was set.
   */
  private keyTimestamp: number = 0;

  /**
   * Refresh every 5 minutes - the default refresh rate.
   */
  private backgroundRefreshInterval = 300000;

  /**
   * Refresh a _max_ of once every minute at steady state.
   *
   * This controls the refresh rate under error conditions.
   */
  private rateLimiter = new LeakyBucket({ maxCapacity: 10, periodMs: 60000 });

  /**
   * Expire keys after an hour, if we failed to refresh in that time.
   */
  private keyExpiry = 3600000;

  private currentErrors: AuthorizationError2[] = [];
  /**
   * Indicates a "fatal" error that should be retried.
   */
  private error = false;

  private refreshPromise: Promise<void> | undefined = undefined;

  constructor(private source: KeyCollector) {}

  async getKeys(): Promise<KeyResult> {
    const now = Date.now();
    if (now - this.keyTimestamp > this.keyExpiry) {
      // Keys have expired - clear
      this.currentKeys = [];
    }

    if (this.wantsRefresh()) {
      // Trigger background refresh.
      // This also sets refreshPromise
      this.refresh();
    }

    if (now - this.keyTimestamp > this.keyExpiry) {
      // Keys have expired - wait for fetching new keys
      // It is possible that the refresh was actually triggered,
      // e.g. in the case of waiting for error retries.
      // In the case of very slow requests, we don't wait for it to complete, but the
      // request can still complete in the background.
      const timeout = timers.setTimeout(3000);
      await Promise.race([this.refreshPromise, timeout]);
    }

    return { keys: this.currentKeys, errors: this.currentErrors };
  }

  private refresh() {
    if (this.refreshPromise == null) {
      if (!this.rateLimiter.allowed()) {
        return;
      }
      this.refreshPromise = this.refreshInner().finally(() => {
        this.refreshPromise = undefined;
      });
    }
    return this.refreshPromise;
  }

  async noKeyFound(): Promise<void> {
    // Refresh keys if allowed by the rate limiter
    await this.refresh();
  }

  private async refreshInner() {
    try {
      const { keys, errors } = await this.source.getKeys();
      // Partial or full result
      this.currentKeys = keys;
      this.currentErrors = errors;
      this.keyTimestamp = Date.now();
      this.error = false;
    } catch (e) {
      this.error = true;
      // No result - keep previous keys
      this.currentErrors = [mapAuthConfigError(e)];
    }
  }

  private wantsRefresh() {
    if (this.error) {
      return true;
    }

    if (Date.now() - this.rateLimiter.lastGrantedRequest >= this.backgroundRefreshInterval) {
      return true;
    }

    return false;
  }

  async addTimeForTests(time: number) {
    this.keyTimestamp -= time;
    this.rateLimiter.reset();
    this.rateLimiter.lastGrantedRequest -= time;
    await this.refreshPromise;
  }
}
