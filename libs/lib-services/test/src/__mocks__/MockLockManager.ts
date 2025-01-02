import { AbstractLockManager } from '../../../src/locks/AbstractLockManager.js';
import { LockActiveError, LockHandle } from '../../../src/locks/LockManager.js';

export class MockLockManager extends AbstractLockManager {
  private currentLockId: string | null = null;
  private lockTimeout?: NodeJS.Timeout;

  /**
   * Acquires a lock if no other lock is active.
   * @returns A promise resolving to the lock ID if successful, or null if a lock is already active.
   */
  async acquireHandle(): Promise<LockHandle | null> {
    if (this.currentLockId) {
      return null;
    }

    const id = this.generateLockId();
    this.currentLockId = id;
    return {
      refresh: () => this.refresh(id),
      release: () => this.release(id)
    };
  }

  /**
   * Refreshes the lock to extend its lifetime.
   * @param lock_id The lock ID to refresh.
   * @throws LockActiveError if the lock ID does not match the active lock.
   */
  protected async refresh(lock_id: string): Promise<void> {
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
  protected async release(lock_id: string): Promise<void> {
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
   * Generates a unique lock ID.
   * @returns A unique string representing the lock ID.
   */
  private generateLockId(): string {
    return Math.random().toString(36).substr(2, 10);
  }
}
