export interface ErrorRateLimiter {
  waitUntilAllowed(options?: { signal?: AbortSignal }): Promise<void>;
  reportError(e: any): void;

  mayPing(): boolean;
}
