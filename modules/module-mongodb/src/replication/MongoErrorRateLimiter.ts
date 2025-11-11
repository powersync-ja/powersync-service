import { ErrorRateLimiter } from '@powersync/service-core';
import { setTimeout } from 'timers/promises';
import { ChangeStreamInvalidatedError } from './ChangeStream.js';

export class MongoErrorRateLimiter implements ErrorRateLimiter {
  nextAllowed: number = Date.now();

  async waitUntilAllowed(options?: { signal?: AbortSignal | undefined } | undefined): Promise<void> {
    const delay = Math.max(0, this.nextAllowed - Date.now());
    // Minimum delay between connections, even without errors (for the next attempt)
    this.setDelay(500);
    await setTimeout(delay, undefined, { signal: options?.signal });
  }

  mayPing(): boolean {
    return Date.now() >= this.nextAllowed;
  }

  reportError(e: any): void {
    // FIXME: Check mongodb-specific requirements
    const message = (e.message as string) ?? '';
    if (e instanceof ChangeStreamInvalidatedError) {
      // Short delay
      this.setDelay(2_000);
    } else if (message.includes('Authentication failed')) {
      // Wait 2 minutes, to avoid triggering too many authentication attempts
      this.setDelay(120_000);
    } else if (message.includes('ENOTFOUND')) {
      // DNS lookup issue - incorrect URI or deleted instance
      this.setDelay(120_000);
    } else if (message.includes('ECONNREFUSED')) {
      // Could be fail2ban or similar
      this.setDelay(120_000);
    } else {
      this.setDelay(5_000);
    }
  }

  private setDelay(delay: number) {
    this.nextAllowed = Math.max(this.nextAllowed, Date.now() + delay);
  }
}
