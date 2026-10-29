import { BucketStorageBatch } from './BucketStorageBatch.js';
import { SourceTable, TableSnapshotStatus } from './SourceTable.js';

/** Bound completion callbacks independently of the storage publication window. */
const MAX_PENDING_SNAPSHOT_PAGES = 2;

/**
 * Tracks page completion without retaining a receipt per page for the entire
 * table. Source queries own their fetched cursor; only onDurable may update
 * caches, metrics or logs that report persisted progress. Dispose before closing
 * the source query or writer so errors and table completion join admitted work.
 */
export class SnapshotProgress implements AsyncDisposable {
  private readonly pending = new Set<Promise<void>>();
  private failure?: { error: unknown };

  constructor(
    private readonly batch: BucketStorageBatch,
    private readonly onDurable: (table: SourceTable) => void
  ) {}

  private throwIfFailed(): void {
    if (this.failure != null) {
      throw this.failure.error;
    }
  }

  /** Call before fetching another page as well as before admitting its progress. */
  async waitForCapacity(): Promise<void> {
    this.throwIfFailed();
    if (this.pending.size >= MAX_PENDING_SNAPSHOT_PAGES) {
      await Promise.race(this.pending);
    }
    this.throwIfFailed();
  }

  async update(table: SourceTable, progress: TableSnapshotStatus): Promise<void> {
    await this.waitForCapacity();
    if (this.batch.queueTableProgress == null) {
      await this.batch.flush();
      this.onDurable(await this.batch.updateTableProgress(table, progress));
      return;
    }
    const receipt = await this.batch.queueTableProgress(table, progress);
    const completion = receipt.durable
      .then((updated) => {
        this.onDurable(updated);
      })
      .catch((error: unknown) => {
        // Observe rejection immediately; surface it on the next source page or
        // during teardown, even if no further rows arrive.
        this.failure ??= { error };
      })
      .finally(() => {
        this.pending.delete(completion);
      });
    this.pending.add(completion);
    this.throwIfFailed();
  }

  async drain(): Promise<void> {
    await Promise.all(this.pending);
    this.throwIfFailed();
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.drain();
  }
}
