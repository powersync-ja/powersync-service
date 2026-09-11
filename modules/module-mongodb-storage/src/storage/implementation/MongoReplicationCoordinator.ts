import { Mutex } from 'async-mutex';

/**
 * Coordinates writers sharing one stream and lease in this process (for example,
 * MongoDB snapshots and CDC). A waiting writer asks the current pipeline to seal
 * and drain, so retaining a partial publication group cannot starve other writers.
 * Cross-process ownership is still enforced by the persisted stream fence.
 */
export class MongoReplicationCoordinator {
  private readonly mutex = new Mutex();
  private waiting = 0;
  private yieldOwner?: () => Promise<void>;

  async acquire(yieldOwner?: () => Promise<void>): Promise<() => void> {
    this.waiting++;
    const acquiring = this.mutex.acquire();
    this.requestYield();
    const release = await acquiring;
    this.waiting--;
    this.yieldOwner = yieldOwner;
    this.requestYield();
    return () => {
      this.yieldOwner = undefined;
      release();
    };
  }

  private requestYield(): void {
    if (this.waiting > 0 && this.yieldOwner != null) {
      // The owning pipeline retains its failure and reports it to its writer.
      void this.yieldOwner().catch(() => {});
    }
  }
}
