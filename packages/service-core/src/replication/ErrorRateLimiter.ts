import { setTimeout } from 'timers/promises';

export enum ErrorType {
  AUTH = 'auth',
  NOT_FOUND = 'not-found',
  CONNECTION_REFUSED = 'connection-refused',
  CONNECTION_CLOSED = 'connection-closed'
}

export interface ErrorRateLimiter {
  waitUntilAllowed(options?: { signal?: AbortSignal }): Promise<void>;
  reportErrorType(type?: ErrorType): void;
  mayPing(): boolean;
}

export class DefaultErrorRateLimiter implements ErrorRateLimiter {
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

  reportErrorType(type?: ErrorType) {
    switch (type) {
      case ErrorType.AUTH:
        this.setDelay(900_000);
        break;
      case ErrorType.NOT_FOUND:
        this.setDelay(120_000);
        break;
      case ErrorType.CONNECTION_REFUSED:
        this.setDelay(120_000);
        break;
      case ErrorType.CONNECTION_CLOSED:
        // Connection timed out - ignore / immediately retry
        // We don't explicitly set the delay to 0, since there could have been another error that
        // we need to respect.
        break;
      default:
        this.setDelay(30_000);
    }
  }

  // TODO implement in runner
  // reportError(e: any): void {
  //   const message = (e.message as string) ?? '';
  //   if (message.includes('password authentication failed')) {
  //     // Wait 15 minutes, to avoid triggering Supabase's fail2ban
  //     this.setDelay(900_000);
  //   } else if (message.includes('ENOTFOUND')) {
  //     // DNS lookup issue - incorrect URI or deleted instance
  //     this.setDelay(120_000);
  //   } else if (message.includes('ECONNREFUSED')) {
  //     // Could be fail2ban or similar
  //     this.setDelay(120_000);
  //   } else if (
  //     message.includes('Unable to do postgres query on ended pool') ||
  //     message.includes('Postgres unexpectedly closed connection')
  //   ) {
  //     // Connection timed out - ignore / immediately retry
  //     // We don't explicitly set the delay to 0, since there could have been another error that
  //     // we need to respect.
  //   } else {
  //     this.setDelay(30_000);
  //   }
  // }

  private setDelay(delay: number) {
    this.nextAllowed = Math.max(this.nextAllowed, Date.now() + delay);
  }
}
