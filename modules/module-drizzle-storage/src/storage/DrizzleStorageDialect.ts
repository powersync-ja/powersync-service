import type { BucketDataRequest } from '@powersync/service-core';
import type { BucketDataRow, sqliteSchema } from '../drivers/sqlite/schema.js';
import type { DrizzleStorageDatabase, DrizzleStorageTransaction } from '../drivers/sqlite/sqlite-config.js';

export interface DrizzleStorageDialect {
  readonly type: string;
  readonly db: DrizzleStorageDatabase;
  readonly tables: typeof sqliteSchema;
  transaction<T>(callback: (tx: DrizzleStorageTransaction) => T): T;
  streamBucketDataRows(options: DrizzleBucketDataStreamOptions): AsyncIterable<BucketDataReadRow>;
  createCheckpointWatcher(): DrizzleCheckpointWatcher;
}

export type BucketDataReadRow = Omit<BucketDataRow, 'id' | 'groupId'>;

export interface DrizzleBucketDataStreamOptions {
  readonly db?: DrizzleStorageDatabase;
  readonly groupId: number;
  readonly checkpoint: bigint;
  readonly dataBuckets: BucketDataRequest[];
  readonly limit: number;
}

export interface DrizzleCheckpointWatcher {
  notify(): void;
  watch(signal: AbortSignal): AsyncIterable<void>;
}

export class InProcessDrizzleCheckpointWatcher implements DrizzleCheckpointWatcher {
  private readonly listeners = new Set<() => void>();

  notify(): void {
    for (const listener of this.listeners) {
      listener();
    }
  }

  async *watch(signal: AbortSignal): AsyncIterable<void> {
    while (!signal.aborted) {
      yield await new Promise<void>((resolve) => {
        const listener = () => {
          this.listeners.delete(listener);
          resolve();
        };
        this.listeners.add(listener);
        signal.addEventListener(
          'abort',
          () => {
            this.listeners.delete(listener);
            resolve();
          },
          { once: true }
        );
      });
    }
  }
}
