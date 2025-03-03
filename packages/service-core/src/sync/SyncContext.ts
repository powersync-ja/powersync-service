import { Semaphore, SemaphoreInterface, withTimeout } from 'async-mutex';

export interface SyncContextOptions {
  maxBuckets: number;
  maxParameterQueryResults: number;
  maxDataFetchConcurrency: number;
}

/**
 * Maximum duration to wait for the mutex to become available.
 *
 * This gives an explicit error if there are mutex issues, rather than just hanging.
 */
const MUTEX_ACQUIRE_TIMEOUT = 30_000;

/**
 * Represents the context in which sync happens.
 *
 * This is global to all sync requests, not per request.
 */
export class SyncContext {
  readonly maxBuckets: number;
  readonly maxParameterQueryResults: number;

  readonly syncSemaphore: SemaphoreInterface;

  constructor(options: SyncContextOptions) {
    this.maxBuckets = options.maxBuckets;
    this.maxParameterQueryResults = options.maxParameterQueryResults;
    this.syncSemaphore = withTimeout(
      new Semaphore(options.maxDataFetchConcurrency),
      MUTEX_ACQUIRE_TIMEOUT,
      new Error(`Timeout while waiting for data`)
    );
  }
}
