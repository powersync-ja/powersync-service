import { logger } from '@powersync/lib-services-framework';
import { storage } from '@powersync/service-core';
import { SyntheticReplicationSource, snapshotPosition } from './SyntheticReplicationSource.js';

export interface SyntheticReplicationStreamOptions {
  readonly source: SyntheticReplicationSource;
  readonly storage: storage.SyncRulesBucketStorage;
  readonly signal: AbortSignal;
}

export class SyntheticReplicationStream {
  constructor(private readonly options: SyntheticReplicationStreamOptions) {}

  async replicate(): Promise<void> {
    await this.options.source.waitForRelease();
    if (this.options.signal.aborted) return;

    try {
      const writer = await this.options.storage.createWriter({
        logger,
        zeroLSN: '00000000000000000000',
        defaultSchema: 'public',
        storeCurrentData: true,
        skipExistingRows: true
      });
      try {
        const resolved = await writer.resolveTables({
          connection_id: 1,
          source: {
            connectionTag: storage.SourceTable.DEFAULT_TAG,
            objectId: 'synthetic-benchmark-items',
            schema: 'public',
            name: 'benchmark_items',
            replicaIdColumns: [{ name: 'id', type: 'VARCHAR', typeId: 25 }]
          }
        });
        if (resolved.tables.length === 0) throw new Error('Synthetic benchmark table did not resolve');

        for (const row of this.options.source.manifest.snapshotRows) {
          this.options.signal.throwIfAborted();
          for (const sourceTable of resolved.tables) {
            await writer.save({
              sourceTable,
              tag: storage.SaveOperationTag.INSERT,
              after: row,
              afterReplicaId: row.id
            });
          }
        }

        const initialPosition = snapshotPosition();
        await writer.markTableSnapshotDone(resolved.tables, initialPosition);
        await writer.markSnapshotDone(initialPosition);
        await writer.commit(initialPosition);
        await this.options.storage.getCheckpoint();
        this.options.source.markSnapshotVisible(initialPosition);

        while (!this.options.signal.aborted) {
          const event = await this.options.source.nextEvent(this.options.signal);
          if (event == null) return;
          if (event.kind === 'keepalive') {
            await writer.keepalive(event.position);
            await this.options.storage.getCheckpoint();
            this.options.source.markTargetVisible(`keepalive-${event.position}`, event.position);
            continue;
          }

          for (const mutation of event.transaction.mutations) {
            for (const sourceTable of resolved.tables) {
              await writer.save({
                sourceTable,
                tag: storage.SaveOperationTag.INSERT,
                after: mutation.row,
                afterReplicaId: mutation.row.id
              });
            }
          }
          await writer.commit(event.transaction.position);
          await this.options.storage.getCheckpoint();
          this.options.source.markTargetVisible(event.transaction.mutations.at(-1)!.row.id, event.transaction.position);
        }
      } finally {
        await writer[Symbol.asyncDispose]();
      }
    } catch (error) {
      this.options.source.fail(error);
      throw error;
    }
  }
}
