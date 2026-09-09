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

  constructor(
    private readonly writer: storage.BucketStorageBatch,
    public table: storage.SourceTable,
    private readonly totalEstimatedCount: number
  ) {}

  /** Call after saving a complete source page. Returns the number of durable rows. */
  async addPage(rows: number, bytes: number, lastKey: Uint8Array | null): Promise<number> {
    this.rows += rows;
    this.bytes += bytes;
    this.pages++;
    this.lastKey = lastKey;
    // Checked at page boundaries: a single source page can overshoot the limit.
    // Storage independently bounds preparation and retained conflict-retry input.
    if (this.rows >= MAX_WINDOW_ROWS || this.bytes >= MAX_WINDOW_BYTES || this.pages >= MAX_WINDOW_PAGES) {
      return this.flush();
    }
    return 0;
  }

  /** Must complete before marking the table snapshot done. Do not flush on failure. */
  async flush(): Promise<number> {
    if (this.rows === 0) return 0;
    await this.writer.flush();
    // Never move the restart cursor ahead of durable data, including conflict retries.
    this.table = await this.writer.updateTableProgress(this.table, {
      lastKey: this.lastKey,
      replicatedCount: (this.table.snapshotStatus?.replicatedCount ?? 0) + this.rows,
      totalEstimatedCount: this.totalEstimatedCount
    });
    const rows = this.rows;
    this.rows = 0;
    this.bytes = 0;
    this.pages = 0;
    return rows;
  }
}
