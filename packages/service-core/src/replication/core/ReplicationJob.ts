import {
  CrudOperation,
  LSNUpdate,
  ReplicationAdapter,
  ReplicationMessage,
  ReplicationMessageType,
  SchemaUpdate,
  TruncateRequest
} from './ReplicationAdapter.js';
import { container, logger } from '@powersync/lib-services-framework';
import { ErrorRateLimiter } from '../ErrorRateLimiter.js';
import { Metrics } from '../../metrics/Metrics.js';
import * as storage from '../../storage/storage-index.js';
import * as util from './replication-utils.js';

export interface ReplicationJobOptions {
  adapter: ReplicationAdapter;
  storage: storage.SyncRulesBucketStorage;
  lock: storage.ReplicationLock;
  rateLimiter?: ErrorRateLimiter;
}

export class ReplicationJob {
  private readonly adapter: ReplicationAdapter;
  private readonly storage: storage.SyncRulesBucketStorage;

  /**
   *  TODO: This will need to be properly sourced once we allow more than 1 connection
   *  @private
   */
  private readonly connectionId = 1;

  private abortController = new AbortController();
  private isReplicatingPromise: Promise<void> | undefined;

  constructor(private options: ReplicationJobOptions) {
    this.adapter = options.adapter;
    this.storage = options.storage;
  }

  isStopped(): boolean {
    return this.abortController.signal.aborted;
  }

  async start() {
    try {
      this.isReplicatingPromise = this.replicateLoop();
    } catch (e) {
      // Fatal exception
      container.reporter.captureException(e, {
        metadata: {
          replicator: this.adapter.name
          // TODO We could allow extra metadata contributed from the adapter here
        }
      });
      logger.error(`Replication failed on ${this.adapter.name}`, e);
    } finally {
      this.abortController.abort();
    }
    await this.options.lock.release();
  }

  async replicateLoop() {
    while (!this.isStopped) {
      await this.replicate();

      if (!this.isStopped) {
        await new Promise((resolve) => setTimeout(resolve, 5000));
      }
    }
  }

  async replicate() {
    try {
      await this.adapter.ensureConfiguration({ abortSignal: this.abortController.signal });
      const status = await this.storage.getStatus();
      const sourceEntities = await this.getSourceEntities();
      if (!(status.snapshot_done && status.checkpoint_lsn)) {
        logger.info(`${this.adapter.name}: Starting initial replication...`);
        await this.storage.clear();
        await this.storage.startBatch({}, async (batch) => {
          await this.initializeData(sourceEntities, batch);
        });
      } else {
        logger.info(`${this.adapter.name}: Initial replication already done.`);
      }
      await this.startReplication(sourceEntities);
    } catch (e) {
      await this.storage.reportError(e);
      throw e;
    }
  }

  private async getSourceEntities(): Promise<storage.SourceTable[]> {
    const resolvedSourceEntities: storage.SourceTable[] = [];

    for (const sourceEntity of this.storage.sync_rules.getSourceTables()) {
      const resolved = await this.adapter.resolveReplicationEntities(sourceEntity);
      for (const resolvedSourceEntity of resolved) {
        const result = await this.storage.resolveTable({
          group_id: this.storage.group_id,
          connection_id: this.connectionId,
          connection_tag: sourceEntity.connectionTag,
          entity_descriptor: resolvedSourceEntity,
          sync_rules: this.storage.sync_rules
        });

        resolvedSourceEntities.push(result.table);
      }
    }

    return resolvedSourceEntities;
  }

  async initializeData(sourceEntities: storage.SourceTable[], storageBatch: storage.BucketStorageBatch) {
    let at = 0;
    let replicatedCount = 0;
    let currentEntity: storage.SourceTable | null = null;
    let estimatedCount = 0n;

    await this.adapter.initializeData({
      entities: sourceEntities,
      abortSignal: this.abortController.signal,
      entryConsumer: async (batch) => {
        await util.touch();

        if (batch.entity != currentEntity) {
          logger.info(`${this.adapter.name}: Replicating: ${batch.entity.table}`);
          currentEntity = batch.entity;
          estimatedCount = await this.adapter.count(currentEntity);
        }

        if (batch.entries.length > 0 && at - replicatedCount >= 5000) {
          logger.info(`${this.adapter.name}: Replicating: ${currentEntity.table} Progress: ${at}/${estimatedCount}`);
        }

        for (const entry of batch.entries) {
          await storageBatch.save({ tag: 'insert', sourceTable: currentEntity, before: undefined, after: entry });
        }
        at += batch.entries.length;
        Metrics.getInstance().rows_replicated_total.add(batch.entries.length);

        if (batch.isLast) {
          await storageBatch.markSnapshotDone([batch.entity], batch.fromLSN);
          await storageBatch.commit(batch.fromLSN);
          at = 0;
          replicatedCount = 0;
          currentEntity = null;
          estimatedCount = -1n;
        }
      }
    });
  }

  async startReplication(sourceEntities: storage.SourceTable[]) {
    await this.storage.autoActivate();

    await this.storage.startBatch({}, async (batch) => {
      await this.adapter.startReplication({
        entities: sourceEntities,
        abortSignal: this.abortController.signal,
        changeListener: (update) => this.handeReplicationUpdate(update, batch)
      });
    });
  }

  private async handeReplicationUpdate(message: ReplicationMessage, batch: storage.BucketStorageBatch): Promise<void> {
    switch (message.type) {
      case ReplicationMessageType.CRUD:
        const crudOperation = message.payload as CrudOperation;
        await batch.save(crudOperation.entry);
        Metrics.getInstance().rows_replicated_total.add(1);
        return;
      case ReplicationMessageType.TRUNCATE:
        const truncateRequest = message.payload as TruncateRequest;
        await batch.truncate(truncateRequest.entities);
        return;
      case ReplicationMessageType.SCHEMA_CHANGE:
        const schemaUpdate = message.payload as SchemaUpdate;
        await this.handleSchemaUpdate(batch, schemaUpdate);
        break;
      case ReplicationMessageType.COMMIT:
        await batch.commit((message.payload as LSNUpdate).lsn!);
        Metrics.getInstance().transactions_replicated_total.add(1);
        return;
      case ReplicationMessageType.KEEP_ALIVE:
        await batch.keepalive((message.payload as LSNUpdate).lsn!);
        return;
    }
  }

  async stop(): Promise<void> {
    logger.info(`Stopping ${this.adapter.name} replication job: ${this.storage.group_id}`);
    // End gracefully
    this.abortController.abort();
    await this.isReplicatingPromise;
  }

  /**
   * Terminate this replication. Cleans up any config for the replication and deletes the replicated data
   *
   * Stops replication if needed.
   * TODO: Confirm if this is still needed at all.
   */
  async terminate(): Promise<void> {
    logger.info(`Terminating ${this.adapter.name} replication job: ${this.storage.group_id}`);
    await this.stop();
    await this.adapter.cleanupReplication();
    await this.options.storage.terminate();
  }

  async handleSchemaUpdate(storageBatch: storage.BucketStorageBatch, update: SchemaUpdate): Promise<void> {
    if (!update.entityDescriptor.objectId) {
      throw new Error('relationId expected');
    }
    const result = await this.storage.resolveTable({
      group_id: this.storage.group_id,
      connection_id: this.connectionId,
      connection_tag: update.connectionTag,
      entity_descriptor: update.entityDescriptor,
      sync_rules: this.storage.sync_rules
    });

    // Drop conflicting tables. This includes for example renamed tables.
    await storageBatch.drop(result.dropTables);

    // Truncate this table, in case a previous snapshot was interrupted.
    await storageBatch.truncate([result.table]);

    await this.initializeData([result.table], storageBatch);
  }
}