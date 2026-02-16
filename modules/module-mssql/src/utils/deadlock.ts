import { logger } from '@powersync/lib-services-framework';
import timers from 'timers/promises';

/**
 * SQL Server deadlock victim error number.
 * When SQL Server detects a deadlock, it chooses one of the participating transactions
 * as the "deadlock victim" and terminates it with error 1205.
 */
const MSSQL_DEADLOCK_ERROR_NUMBER = 1205;
const MSSQL_DEADLOCK_RETRIES = 5;
const MSSQL_DEADLOCK_BACKOFF_FACTOR = 2;
const MSSQL_DEADLOCK_RETRY_DELAY_MS = 200;
const MSSQL_DEADLOCK_MAX_DELAY_MS = 5000;

/**
 * Retries the given async function if it fails with a SQL Server deadlock error (1205).
 * Uses exponential backoff between retries.
 *
 * If the error is not a deadlock or all retries are exhausted, the error is re-thrown.
 */
export async function retryOnDeadlock<T>(fn: () => Promise<T>, operationName: string): Promise<T> {
  let lastError: Error | null = null;
  for (let attempt = 0; attempt <= MSSQL_DEADLOCK_RETRIES; attempt++) {
    try {
      return await fn();
    } catch (error) {
      lastError = error;
      if (!isDeadlockError(error) || attempt === MSSQL_DEADLOCK_RETRIES) {
        throw error;
      }
      const delay = Math.min(
        MSSQL_DEADLOCK_RETRY_DELAY_MS * Math.pow(MSSQL_DEADLOCK_BACKOFF_FACTOR, attempt),
        MSSQL_DEADLOCK_MAX_DELAY_MS
      );
      logger.warn(
        `Deadlock detected during ${operationName} (attempt ${attempt + 1}/${MSSQL_DEADLOCK_RETRIES + 1}). Retrying in ${delay}ms...`
      );
      await timers.setTimeout(delay);
    }
  }

  throw lastError;
}

export function isDeadlockError(error: unknown): boolean {
  if (error != null && typeof error === 'object' && 'number' in error) {
    return (error as { number: unknown }).number === MSSQL_DEADLOCK_ERROR_NUMBER;
  }
  return false;
}
