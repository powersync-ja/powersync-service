import { ErrorRateLimiter } from '@powersync/service-core';
import { setTimeout } from 'timers/promises';
import { ConvexApiError } from '../client/ConvexApiClient.js';

export class ConvexErrorRateLimiter implements ErrorRateLimiter {
  private nextAllowed = Date.now();

  async waitUntilAllowed(options?: { signal?: AbortSignal | undefined } | undefined): Promise<void> {
    const delay = Math.max(0, this.nextAllowed - Date.now());
    this.setDelay(500);
    await setTimeout(delay, undefined, { signal: options?.signal });
  }

  mayPing(): boolean {
    return Date.now() >= this.nextAllowed;
  }

  reportError(error: any): void {
    if (error instanceof ConvexApiError) {
      if (error.status == 401 || error.status == 403) {
        this.setDelay(120_000);
        return;
      }
      if (error.status == 429) {
        this.setDelay(15_000);
        return;
      }
      if (error.retryable) {
        this.setDelay(10_000);
        return;
      }
      this.setDelay(45_000);
      return;
    }

    const message = (error?.message as string | undefined) ?? '';
    if (message.includes('ENOTFOUND') || message.includes('ECONNREFUSED')) {
      this.setDelay(120_000);
    } else {
      this.setDelay(20_000);
    }
  }

  private setDelay(delay: number) {
    this.nextAllowed = Math.max(this.nextAllowed, Date.now() + delay);
  }
}
