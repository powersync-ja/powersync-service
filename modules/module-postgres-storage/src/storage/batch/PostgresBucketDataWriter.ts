import * as lib_postgres from '@powersync/lib-service-postgres';
import { Logger, ReplicationAssertionError } from '@powersync/lib-services-framework';
import {
  BatchedCustomWriteCheckpointOptions,
  BucketStorageMarkRecordUnavailable,
  maxLsn,
  storage
} from '@powersync/service-core';
import { RowProcessor } from '@powersync/service-sync-rules';
import { models } from '../../types/types.js';
import { PostgresBucketBatch } from './PostgresBucketBatch.js';
import { postgresTableId } from './PostgresPersistedBatch.js';
import { PostgresSourceTable } from '../PostgresSourceTable.js';

export interface PostgresWriterOptions {
  db: lib_postgres.DatabaseClient;
  rowProcessor: RowProcessor;
  storeCurrentData: boolean;
  skipExistingRows: boolean;
  logger?: Logger;
  markRecordUnavailable?: BucketStorageMarkRecordUnavailable;
}

export class PostgresBucketDataWriter implements storage.BucketDataWriter {
  public readonly rowProcessor: RowProcessor;
  write_checkpoint_batch: storage.CustomWriteCheckpointOptions[] = [];

  protected db: lib_postgres.DatabaseClient;

  public subWriters: PostgresBucketBatch[] = [];

  constructor(options: PostgresWriterOptions) {
    this.db = options.db;
    this.rowProcessor = options.rowProcessor;
  }

  addSubWriter(subWriter: PostgresBucketBatch) {
    this.subWriters.push(subWriter);
  }

  get resumeFromLsn(): string | null {
    // FIXME: check the logic here when there are multiple batches
    let lsn: string | null = null;
    for (let sub of this.subWriters) {
      // TODO: should this be min instead?
      lsn = maxLsn(lsn, sub.resumeFromLsn);
    }
    return lsn;
  }

  async keepalive(lsn: string): Promise<boolean> {
    let didAny = false;
    for (let batch of this.subWriters) {
      const didBatchKeepalive = await batch.keepalive(lsn);
      didAny ||= didBatchKeepalive;
    }
    return didAny;
  }

  async commit(lsn: string, options?: storage.BucketBatchCommitOptions): Promise<boolean> {
    let didCommit = false;
    for (let batch of this.subWriters) {
      const didWriterCommit = await batch.commit(lsn, options);
      didCommit ||= didWriterCommit;
    }
    return didCommit;
  }

  async setResumeLsn(lsn: string): Promise<void> {
    for (let batch of this.subWriters) {
      await batch.setResumeLsn(lsn);
    }
  }

  async resolveTables(options: storage.ResolveTablesOptions): Promise<storage.SourceTable[]> {
    let result: storage.SourceTable[] = [];
    for (let subWriter of this.subWriters) {
      const subResult = await subWriter.storage.resolveTable({
        connection_id: options.connection_id,
        connection_tag: options.connection_tag,
        entity_descriptor: options.entity_descriptor,
        sync_rules: subWriter.sync_rules,
        idGenerator: options.idGenerator
      });
      result.push(subResult.table);
    }
    return result;
  }

  async resolveTablesToDrop(options: storage.ResolveTableToDropsOptions): Promise<storage.SourceTable[]> {
    // FIXME: remove the duplicate work between this and resolveTables()
    let result: storage.SourceTable[] = [];
    for (let subWriter of this.subWriters) {
      const subResult = await subWriter.storage.resolveTable({
        connection_id: options.connection_id,
        connection_tag: options.connection_tag,
        entity_descriptor: options.entity_descriptor,
        sync_rules: subWriter.sync_rules
      });
      result.push(...subResult.dropTables);
    }
    return result;
  }

  private subWriterForTable(table: storage.SourceTable): PostgresBucketBatch {
    // FIXME: store on the SourceTable instead?
    if (!(table instanceof PostgresSourceTable)) {
      throw new ReplicationAssertionError(`Source table is not a PostgresSourceTable`);
    }
    const subWriter = this.subWriters.find((sw) => sw.storage.group_id === table.groupId);
    if (subWriter == null) {
      throw new ReplicationAssertionError(
        `No sub-writer found for source table ${table.qualifiedName} with group ID ${table.groupId}`
      );
    }

    return subWriter;
  }

  async getTable(ref: storage.SourceTable): Promise<storage.SourceTable | null> {
    const sourceTableRow = await this.db.sql`
      SELECT
        *
      FROM
        source_tables
      WHERE
        id = ${{ type: 'varchar', value: postgresTableId(ref.id) }}
    `
      .decoded(models.SourceTable)
      .first();
    if (sourceTableRow == null) {
      return null;
    }

    const subWriter = this.subWriters.find((sw) => sw.storage.group_id === sourceTableRow.group_id);
    if (subWriter == null) {
      throw new ReplicationAssertionError(
        `No sub-writer found for source table ${ref.qualifiedName} with group ID ${sourceTableRow.group_id}`
      );
    }

    const sourceTable = new PostgresSourceTable(
      {
        // Immutable values
        id: sourceTableRow.id,
        connectionTag: ref.connectionTag,
        objectId: ref.objectId,
        schema: ref.schema,
        name: ref.name,
        replicaIdColumns: ref.replicaIdColumns,
        pattern: ref.pattern,

        // Table state
        snapshotComplete: sourceTableRow!.snapshot_done ?? true
      },
      { groupId: sourceTableRow.group_id }
    );
    if (!sourceTable.snapshotComplete) {
      sourceTable.snapshotStatus = {
        totalEstimatedCount: Number(sourceTableRow!.snapshot_total_estimated_count ?? -1n),
        replicatedCount: Number(sourceTableRow!.snapshot_replicated_count ?? 0n),
        lastKey: sourceTableRow!.snapshot_last_key
      };
    }
    // Immutable
    sourceTable.syncEvent = ref.syncEvent;
    sourceTable.syncData = ref.syncData;
    sourceTable.syncParameters = ref.syncParameters;
    return sourceTable;
  }

  async save(record: storage.SaveOptions): Promise<storage.FlushedResult | null> {
    const writer = this.subWriterForTable(record.sourceTable);
    return writer.save(record);
  }

  async truncate(sourceTables: storage.SourceTable[]): Promise<storage.FlushedResult | null> {
    let flushedResult: storage.FlushedResult | null = null;
    for (let table of sourceTables) {
      const writer = this.subWriterForTable(table);
      const subResult = await writer.truncate([table]);
      flushedResult = maxFlushedResult(flushedResult, subResult);
    }
    return flushedResult;
  }

  async drop(sourceTables: storage.SourceTable[]): Promise<storage.FlushedResult | null> {
    let flushedResult: storage.FlushedResult | null = null;
    for (let table of sourceTables) {
      const writer = this.subWriterForTable(table);
      const subResult = await writer.drop([table]);
      flushedResult = maxFlushedResult(flushedResult, subResult);
    }
    return flushedResult;
  }

  async flush(options?: storage.BatchBucketFlushOptions): Promise<storage.FlushedResult | null> {
    let flushedResult: storage.FlushedResult | null = null;
    for (let writer of this.subWriters) {
      const subResult = await writer.flush();
      flushedResult = maxFlushedResult(flushedResult, subResult);
    }
    return flushedResult;
  }

  async markTableSnapshotDone(
    tables: storage.SourceTable[],
    no_checkpoint_before_lsn?: string
  ): Promise<storage.SourceTable[]> {
    let result: storage.SourceTable[] = [];
    for (let table of tables) {
      const writer = this.subWriterForTable(table);
      const mapped = await writer.markTableSnapshotDone([table], no_checkpoint_before_lsn);
      result.push(...mapped);
    }
    return result;
  }

  async markTableSnapshotRequired(table: storage.SourceTable): Promise<void> {
    const writer = this.subWriterForTable(table);
    await writer.markTableSnapshotRequired(table);
  }

  async markAllSnapshotDone(no_checkpoint_before_lsn: string): Promise<void> {
    for (let writer of this.subWriters) {
      await writer.markAllSnapshotDone(no_checkpoint_before_lsn);
    }
  }

  async updateTableProgress(
    table: storage.SourceTable,
    progress: Partial<storage.TableSnapshotStatus>
  ): Promise<storage.SourceTable> {
    const writer = this.subWriterForTable(table);
    return await writer.updateTableProgress(table, progress);
  }

  /**
   * Queues the creation of a custom Write Checkpoint. This will be persisted after operations are flushed.
   */
  addCustomWriteCheckpoint(checkpoint: BatchedCustomWriteCheckpointOptions): void {
    for (let writer of this.subWriters) {
      writer.addCustomWriteCheckpoint(checkpoint);
    }
  }

  async [Symbol.asyncDispose]() {
    for (let writer of this.subWriters) {
      await writer[Symbol.asyncDispose]();
    }
  }
}

function maxFlushedResult(
  a: storage.FlushedResult | null,
  b: storage.FlushedResult | null
): storage.FlushedResult | null {
  if (a == null) {
    return b;
  }
  if (b == null) {
    return a;
  }
  return a.flushed_op > b.flushed_op ? a : b;
}
