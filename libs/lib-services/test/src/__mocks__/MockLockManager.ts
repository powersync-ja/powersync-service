import { LockActiveError, LockManager } from '../../../src/locks/LockManager.js';

export class MockLockManager implements LockManager {
  private currentLockId: string | null = null;
  private lockTimeout?: NodeJS.Timeout;

  /**
   * Acquires a lock if no other lock is active.
   * @returns A promise resolving to the lock ID if successful, or null if a lock is already active.
   */
  async acquire(): Promise<string | null> {
    if (this.currentLockId) {
      return null;
    }

    this.currentLockId = this.generateLockId();
    return this.currentLockId;
  }

  /**
   * Refreshes the lock to extend its lifetime.
   * @param lock_id The lock ID to refresh.
   * @throws LockActiveError if the lock ID does not match the active lock.
   */
  async refresh(lock_id: string): Promise<void> {
    if (this.currentLockId !== lock_id) {
      throw new LockActiveError();
    }

    // Simulate refreshing the lock (e.g., resetting a timeout).
    if (this.lockTimeout) {
      clearTimeout(this.lockTimeout);
    }
    this.lockTimeout = setTimeout(() => {
      this.currentLockId = null;
    }, 30000); // Example: 30 seconds lock lifetime.
  }

  /**
   * Releases the active lock.
   * @param lock_id The lock ID to release.
   * @throws LockActiveError if the lock ID does not match the active lock.
   */
  async release(lock_id: string): Promise<void> {
    if (this.currentLockId !== lock_id) {
      throw new LockActiveError();
    }

    this.currentLockId = null;
    if (this.lockTimeout) {
      clearTimeout(this.lockTimeout);
      this.lockTimeout = undefined;
    }
  }

  /**
   * Wraps a handler function in a lock, ensuring only one execution at a time.
   * @param handler A function to execute while the lock is active.
   * @returns A promise that resolves once the handler completes.
   */
  async lock(handler: (refresh: () => Promise<void>) => Promise<void>): Promise<void> {
    const lockId = await this.acquire();
    if (!lockId) {
      throw new LockActiveError();
    }

    try {
      await handler(async () => await this.refresh(lockId));
    } finally {
      await this.release(lockId);
    }
  }

  /**
   * Generates a unique lock ID.
   * @returns A unique string representing the lock ID.
   */
  private generateLockId(): string {
    return Math.random().toString(36).substr(2, 10);
  }
}
