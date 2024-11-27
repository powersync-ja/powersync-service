import { ErrorRateLimiter } from '@powersync/service-core';
import { setTimeout } from 'timers/promises';

export class MySQLErrorRateLimiter implements ErrorRateLimiter {
  nextAllowed: number = Date.now();

  async waitUntilAllowed(options?: { signal?: AbortSignal | undefined } | undefined): Promise<void> {
    const delay = Math.max(0, this.nextAllowed - Date.now());
    // Minimum delay between connections, even without errors
    this.setDelay(500);
    await setTimeout(delay, undefined, { signal: options?.signal });
  }

  mayPing(): boolean {
    return Date.now() >= this.nextAllowed;
  }

  reportError(e: any): void {
    const message = (e.message as string) ?? '';
    if (message.includes('password authentication failed')) {
      // Wait 15 minutes, to avoid triggering Supabase's fail2ban
      this.setDelay(900_000);
    } else if (message.includes('ENOTFOUND')) {
      // DNS lookup issue - incorrect URI or deleted instance
      this.setDelay(120_000);
    } else if (message.includes('ECONNREFUSED')) {
      // Could be fail2ban or similar
      this.setDelay(120_000);
    } else {
      this.setDelay(30_000);
    }
  }

  private setDelay(delay: number) {
    this.nextAllowed = Math.max(this.nextAllowed, Date.now() + delay);
  }
}
