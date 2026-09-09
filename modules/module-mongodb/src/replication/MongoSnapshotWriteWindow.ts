import { storage } from '@powersync/service-core';

// Source cursor pages remain small for prefetching. Amortize publication and
// progress transactions over several pages, bounded by both rows and raw bytes.
const MAX_WINDOW_ROWS = 24_000;
const MAX_WINDOW_BYTES = 16 * 1024 * 1024;
// Keep restart progress reasonably frequent when operators choose small pages.
const MAX_WINDOW_PAGES = 4;

export class MongoSnapshotWriteWindow {
  private rows = 0;
  private bytes = 0;
  private pages = 0;
  private lastKey: Uint8Array | null = null;
  private admittedTable: storage.SourceTable;
  private completedRows = 0;
  private readonly pending = new Set<Promise<void>>();
  private failure: unknown;

  constructor(
    private readonly writer: storage.BucketStorageBatch,
    public table: storage.SourceTable,
    private readonly totalEstimatedCount: number
  ) {
    this.admittedTable = table;
  }

  /** Call after saving a complete source page. Returns the number of durable rows. */
  async addPage(rows: number, bytes: number, lastKey: Uint8Array | null): Promise<number> {
    if (this.failure) throw this.failure;
    this.rows += rows;
    this.bytes += bytes;
    this.pages++;
    this.lastKey = lastKey;
    // Checked at page boundaries: a single source page can overshoot the limit.
    // Storage independently bounds preparation and retained conflict-retry input.
    if (this.rows >= MAX_WINDOW_ROWS || this.bytes >= MAX_WINDOW_BYTES || this.pages >= MAX_WINDOW_PAGES) {
      await this.admit();
    }
    return this.takeCompletedRows();
  }

  /** Must complete before marking the table snapshot done. Do not flush on failure. */
  async flush(): Promise<number> {
    if (this.failure) throw this.failure;
    await this.admit();
    if (this.pending.size > 0) {
      await this.writer.flush();
      await Promise.all(this.pending);
    }
    if (this.failure) throw this.failure;
    return this.takeCompletedRows();
  }

  private takeCompletedRows() {
    const rows = this.completedRows;
    this.completedRows = 0;
    return rows;
  }

  private async admit() {
    if (this.rows === 0) return;
    const progress = {
      lastKey: this.lastKey,
      replicatedCount: (this.admittedTable.snapshotStatus?.replicatedCount ?? 0) + this.rows,
      totalEstimatedCount: this.totalEstimatedCount
    };
    let receipt: { table: storage.SourceTable; persisted: Promise<void> };
    if (this.writer.queueTableProgress) {
      receipt = await this.writer.queueTableProgress(this.admittedTable, progress);
    } else {
      await this.writer.flush();
      receipt = {
        table: await this.writer.updateTableProgress(this.admittedTable, progress),
        persisted: Promise.resolve()
      };
    }
    this.admittedTable = receipt.table;
    const rows = this.rows;
    this.rows = 0;
    this.bytes = 0;
    this.pages = 0;
    const persisted = receipt.persisted.then(() => {
      // Logging/metrics use durable progress, never merely admitted progress.
      this.table = receipt.table;
      this.completedRows += rows;
    });
    this.pending.add(persisted);
    void persisted.then(
      () => this.pending.delete(persisted),
      (error) => {
        this.failure = error;
        this.pending.delete(persisted);
      }
    );
  }
}
