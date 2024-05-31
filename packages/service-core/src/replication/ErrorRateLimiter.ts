import { setTimeout } from 'timers/promises';

export interface ErrorRateLimiter {
  waitUntilAllowed(options?: { signal?: AbortSignal }): Promise<void>;
  reportError(e: any): void;

  mayPing(): boolean;
}

export class DefaultErrorRateLimiter implements ErrorRateLimiter {
  nextAllowed: number = Date.now();

  async waitUntilAllowed(options?: { signal?: AbortSignal | undefined } | undefined): Promise<void> {
    const delay = Math.max(0, this.nextAllowed - Date.now());
    this.setDelay(5_000);
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
    } else if (
      message.includes('Unable to do postgres query on ended pool') ||
      message.includes('Postgres unexpectedly closed connection')
    ) {
      // Connection timed out - ignore / immediately retry
      // We don't explicitly set the delay to 0, since there could have been another error that
      // we need to respect.
    } else {
      this.setDelay(30_000);
    }
  }

  private setDelay(delay: number) {
    this.nextAllowed = Math.max(this.nextAllowed, Date.now() + delay);
  }
}
