import * as framework from '@powersync/lib-services-framework';
import { storage, sync, utils } from '@powersync/service-core';
import * as pg_wire from '@powersync/service-jpgwire';
import * as sync_rules from '@powersync/service-sync-rules';
import crypto from 'crypto';
import { wrapWithAbort } from 'ix/asynciterable/operators/withabort.js';
import { LRUCache } from 'lru-cache/min';
import * as timers from 'timers/promises';
import * as uuid from 'uuid';

import * as lib_postgres from '@powersync/lib-service-postgres';
import { models, NormalizedPostgresStorageConfig } from '../types/types.js';

import { NOTIFICATION_CHANNEL, STORAGE_SCHEMA_NAME } from '../utils/db.js';
import { notifySyncRulesUpdate } from './batch/PostgresBucketBatch.js';
import { PostgresSyncRulesStorage } from './PostgresSyncRulesStorage.js';
import { PostgresPersistedSyncRulesContent } from './sync-rules/PostgresPersistedSyncRulesContent.js';

export type PostgresBucketStorageOptions = {
  config: NormalizedPostgresStorageConfig;
  slot_name_prefix: string;
};

export class PostgresBucketStorageFactory
  extends framework.DisposableObserver<storage.BucketStorageFactoryListener>
  implements storage.BucketStorageFactory
{
  readonly db: lib_postgres.DatabaseClient;
  public readonly slot_name_prefix: string;

  private sharedIterator = new sync.BroadcastIterable((signal) => this.watchActiveCheckpoint(signal));

  private readonly storageCache = new LRUCache<number, storage.SyncRulesBucketStorage>({
    max: 3,
    fetchMethod: async (id) => {
      const syncRulesRow = await this.db.sql`
        SELECT
          *
        FROM
          sync_rules
        WHERE
          id = ${{ value: id, type: 'int4' }}
      `
        .decoded(models.SyncRules)
        .first();
      if (syncRulesRow == null) {
        // Deleted in the meantime?
        return undefined;
      }
      const rules = new PostgresPersistedSyncRulesContent(this.db, syncRulesRow);
      return this.getInstance(rules);
    },
    dispose: (storage) => {
      storage[Symbol.dispose]();
    }
  });

  constructor(protected options: PostgresBucketStorageOptions) {
    super();
    this.db = new lib_postgres.DatabaseClient({
      config: options.config,
      schema: STORAGE_SCHEMA_NAME,
      notificationChannels: [NOTIFICATION_CHANNEL]
    });
    this.slot_name_prefix = options.slot_name_prefix;

    this.db.registerListener({
      connectionCreated: async (connection) => this.prepareStatements(connection)
    });
  }

  async [Symbol.asyncDispose]() {
    super[Symbol.dispose]();
    await this.db[Symbol.asyncDispose]();
  }

  async prepareStatements(connection: pg_wire.PgConnection) {
    // It should be possible to prepare statements for some common operations here.
    // This has not been implemented yet.
  }

  getInstance(syncRules: storage.PersistedSyncRulesContent): storage.SyncRulesBucketStorage {
    const storage = new PostgresSyncRulesStorage({
      factory: this,
      db: this.db,
      sync_rules: syncRules,
      batchLimits: this.options.config.batch_limits
    });
    this.iterateListeners((cb) => cb.syncStorageCreated?.(storage));
    storage.registerListener({
      batchStarted: (batch) => {
        // This nested listener will be automatically disposed when the storage is disposed
        batch.registerManagedListener(storage, {
          replicationEvent: (payload) => this.iterateListeners((cb) => cb.replicationEvent?.(payload))
        });
      }
    });
    return storage;
  }

  async getStorageMetrics(): Promise<storage.StorageMetrics> {
    const active_sync_rules = await this.getActiveSyncRules({ defaultSchema: 'public' });
    if (active_sync_rules == null) {
      return {
        operations_size_bytes: 0,
        parameters_size_bytes: 0,
        replication_size_bytes: 0
      };
    }

    const sizes = await this.db.sql`
      SELECT
        pg_total_relation_size('current_data') AS current_size_bytes,
        pg_total_relation_size('bucket_parameters') AS parameter_size_bytes,
        pg_total_relation_size('bucket_data') AS operations_size_bytes;
    `.first<{ current_size_bytes: bigint; parameter_size_bytes: bigint; operations_size_bytes: bigint }>();

    return {
      operations_size_bytes: Number(sizes!.operations_size_bytes),
      parameters_size_bytes: Number(sizes!.parameter_size_bytes),
      replication_size_bytes: Number(sizes!.current_size_bytes)
    };
  }

  async getPowerSyncInstanceId(): Promise<string> {
    const instanceRow = await this.db.sql`
      SELECT
        id
      FROM
        instance
    `
      .decoded(models.Instance)
      .first();
    if (instanceRow) {
      return instanceRow.id;
    }
    const lockManager = new lib_postgres.PostgresLockManager({
      db: this.db,
      name: `instance-id-insertion-lock`
    });
    await lockManager.lock(async () => {
      await this.db.sql`
        INSERT INTO
          instance (id)
        VALUES
          (${{ type: 'varchar', value: uuid.v4() }})
      `.execute();
    });
    const newInstanceRow = await this.db.sql`
      SELECT
        id
      FROM
        instance
    `
      .decoded(models.Instance)
      .first();
    return newInstanceRow!.id;
  }

  // TODO possibly share implementation in abstract class
  async configureSyncRules(
    sync_rules: string,
    options?: { lock?: boolean }
  ): Promise<{
    updated: boolean;
    persisted_sync_rules?: storage.PersistedSyncRulesContent;
    lock?: storage.ReplicationLock;
  }> {
    const next = await this.getNextSyncRulesContent();
    const active = await this.getActiveSyncRulesContent();

    if (next?.sync_rules_content == sync_rules) {
      framework.logger.info('Sync rules from configuration unchanged');
      return { updated: false };
    } else if (next == null && active?.sync_rules_content == sync_rules) {
      framework.logger.info('Sync rules from configuration unchanged');
      return { updated: false };
    } else {
      framework.logger.info('Sync rules updated from configuration');
      const persisted_sync_rules = await this.updateSyncRules({
        content: sync_rules,
        lock: options?.lock
      });
      return { updated: true, persisted_sync_rules, lock: persisted_sync_rules.current_lock ?? undefined };
    }
  }

  async updateSyncRules(options: storage.UpdateSyncRulesOptions): Promise<PostgresPersistedSyncRulesContent> {
    // TODO some shared implementation for this might be nice
    // Parse and validate before applying any changes
    sync_rules.SqlSyncRules.fromYaml(options.content, {
      // No schema-based validation at this point
      schema: undefined,
      defaultSchema: 'not_applicable', // Not needed for validation
      throwOnError: true
    });

    return this.db.transaction(async (db) => {
      await db.sql`
        UPDATE sync_rules
        SET
          state = ${{ type: 'varchar', value: storage.SyncRuleState.STOP }}
        WHERE
          state = ${{ type: 'varchar', value: storage.SyncRuleState.PROCESSING }}
      `.execute();

      const newSyncRulesRow = await db.sql`
        WITH
          next_id AS (
            SELECT
              nextval('sync_rules_id_sequence') AS id
          )
        INSERT INTO
          sync_rules (id, content, state, slot_name)
        VALUES
          (
            (
              SELECT
                id
              FROM
                next_id
            ),
            ${{ type: 'varchar', value: options.content }},
            ${{ type: 'varchar', value: storage.SyncRuleState.PROCESSING }},
            CONCAT(
              ${{ type: 'varchar', value: this.slot_name_prefix }},
              (
                SELECT
                  id
                FROM
                  next_id
              ),
              '_',
              ${{ type: 'varchar', value: crypto.randomBytes(2).toString('hex') }}
            )
          )
        RETURNING
          *
      `
        .decoded(models.SyncRules)
        .first();

      await notifySyncRulesUpdate(this.db, newSyncRulesRow!);

      return new PostgresPersistedSyncRulesContent(this.db, newSyncRulesRow!);
    });
  }

  async slotRemoved(slot_name: string): Promise<void> {
    const next = await this.getNextSyncRulesContent();
    const active = await this.getActiveSyncRulesContent();

    // In both the below cases, we create a new sync rules instance.
    // The current one will continue erroring until the next one has finished processing.
    if (next != null && next.slot_name == slot_name) {
      // We need to redo the "next" sync rules
      await this.updateSyncRules({
        content: next.sync_rules_content
      });
      // Pro-actively stop replicating
      await this.db.sql`
        UPDATE sync_rules
        SET
          state = ${{ value: storage.SyncRuleState.STOP, type: 'varchar' }}
        WHERE
          id = ${{ value: next.id, type: 'int4' }}
          AND state = ${{ value: storage.SyncRuleState.PROCESSING, type: 'varchar' }}
      `.execute();
    } else if (next == null && active?.slot_name == slot_name) {
      // Slot removed for "active" sync rules, while there is no "next" one.
      await this.updateSyncRules({
        content: active.sync_rules_content
      });

      // Pro-actively stop replicating
      await this.db.sql`
        UPDATE sync_rules
        SET
          state = ${{ value: storage.SyncRuleState.STOP, type: 'varchar' }}
        WHERE
          id = ${{ value: active.id, type: 'int4' }}
          AND state = ${{ value: storage.SyncRuleState.ACTIVE, type: 'varchar' }}
      `.execute();
    }
  }

  // TODO possibly share via abstract class
  async getActiveSyncRules(options: storage.ParseSyncRulesOptions): Promise<storage.PersistedSyncRules | null> {
    const content = await this.getActiveSyncRulesContent();
    return content?.parsed(options) ?? null;
  }

  async getActiveSyncRulesContent(): Promise<storage.PersistedSyncRulesContent | null> {
    const activeRow = await this.db.sql`
      SELECT
        *
      FROM
        sync_rules
      WHERE
        state = ${{ value: storage.SyncRuleState.ACTIVE, type: 'varchar' }}
      ORDER BY
        id DESC
      LIMIT
        1
    `
      .decoded(models.SyncRules)
      .first();
    if (!activeRow) {
      return null;
    }

    return new PostgresPersistedSyncRulesContent(this.db, activeRow);
  }

  // TODO possibly share via abstract class
  async getNextSyncRules(options: storage.ParseSyncRulesOptions): Promise<storage.PersistedSyncRules | null> {
    const content = await this.getNextSyncRulesContent();
    return content?.parsed(options) ?? null;
  }

  async getNextSyncRulesContent(): Promise<storage.PersistedSyncRulesContent | null> {
    const nextRow = await this.db.sql`
      SELECT
        *
      FROM
        sync_rules
      WHERE
        state = ${{ value: storage.SyncRuleState.PROCESSING, type: 'varchar' }}
      ORDER BY
        id DESC
      LIMIT
        1
    `
      .decoded(models.SyncRules)
      .first();
    if (!nextRow) {
      return null;
    }

    return new PostgresPersistedSyncRulesContent(this.db, nextRow);
  }

  async getReplicatingSyncRules(): Promise<storage.PersistedSyncRulesContent[]> {
    const rows = await this.db.sql`
      SELECT
        *
      FROM
        sync_rules
      WHERE
        state = ${{ value: storage.SyncRuleState.ACTIVE, type: 'varchar' }}
        OR state = ${{ value: storage.SyncRuleState.PROCESSING, type: 'varchar' }}
    `
      .decoded(models.SyncRules)
      .rows();

    return rows.map((row) => new PostgresPersistedSyncRulesContent(this.db, row));
  }

  async getStoppedSyncRules(): Promise<storage.PersistedSyncRulesContent[]> {
    const rows = await this.db.sql`
      SELECT
        *
      FROM
        sync_rules
      WHERE
        state = ${{ value: storage.SyncRuleState.STOP, type: 'varchar' }}
    `
      .decoded(models.SyncRules)
      .rows();

    return rows.map((row) => new PostgresPersistedSyncRulesContent(this.db, row));
  }

  async getActiveCheckpoint(): Promise<storage.ActiveCheckpoint> {
    const activeCheckpoint = await this.db.sql`
      SELECT
        id,
        last_checkpoint,
        last_checkpoint_lsn
      FROM
        sync_rules
      WHERE
        state = ${{ value: storage.SyncRuleState.ACTIVE, type: 'varchar' }}
      ORDER BY
        id DESC
      LIMIT
        1
    `
      .decoded(models.ActiveCheckpoint)
      .first();

    return this.makeActiveCheckpoint(activeCheckpoint);
  }

  async *watchWriteCheckpoint(user_id: string, signal: AbortSignal): AsyncIterable<storage.WriteCheckpoint> {
    let lastCheckpoint: utils.OpId | null = null;
    let lastWriteCheckpoint: bigint | null = null;

    const iter = wrapWithAbort(this.sharedIterator, signal);
    for await (const cp of iter) {
      const { checkpoint, lsn } = cp;

      // lsn changes are not important by itself.
      // What is important is:
      // 1. checkpoint (op_id) changes.
      // 2. write checkpoint changes for the specific user
      const bucketStorage = await cp.getBucketStorage();
      if (!bucketStorage) {
        continue;
      }

      const lsnFilters: Record<string, string> = lsn ? { 1: lsn } : {};

      const currentWriteCheckpoint = await bucketStorage.lastWriteCheckpoint({
        user_id,
        heads: {
          ...lsnFilters
        }
      });

      if (currentWriteCheckpoint == lastWriteCheckpoint && checkpoint == lastCheckpoint) {
        // No change - wait for next one
        // In some cases, many LSNs may be produced in a short time.
        // Add a delay to throttle the write checkpoint lookup a bit.
        await timers.setTimeout(20 + 10 * Math.random());
        continue;
      }

      lastWriteCheckpoint = currentWriteCheckpoint;
      lastCheckpoint = checkpoint;

      yield { base: cp, writeCheckpoint: currentWriteCheckpoint };
    }
  }

  protected async *watchActiveCheckpoint(signal: AbortSignal): AsyncIterable<storage.ActiveCheckpoint> {
    const doc = await this.db.sql`
      SELECT
        id,
        last_checkpoint,
        last_checkpoint_lsn
      FROM
        sync_rules
      WHERE
        state = ${{ type: 'varchar', value: storage.SyncRuleState.ACTIVE }}
      LIMIT
        1
    `
      .decoded(models.ActiveCheckpoint)
      .first();

    const sink = new sync.LastValueSink<string>(undefined);

    const disposeListener = this.db.registerListener({
      notification: (notification) => sink.next(notification.payload)
    });

    signal.addEventListener('aborted', async () => {
      disposeListener();
      sink.complete();
    });

    yield this.makeActiveCheckpoint(doc);

    let lastOp: storage.ActiveCheckpoint | null = null;
    for await (const payload of sink.withSignal(signal)) {
      if (signal.aborted) {
        return;
      }

      const notification = models.ActiveCheckpointNotification.decode(payload);
      const activeCheckpoint = this.makeActiveCheckpoint(notification.active_checkpoint);

      if (lastOp == null || activeCheckpoint.lsn != lastOp.lsn || activeCheckpoint.checkpoint != lastOp.checkpoint) {
        lastOp = activeCheckpoint;
        yield activeCheckpoint;
      }
    }
  }

  private makeActiveCheckpoint(row: models.ActiveCheckpointDecoded | null) {
    return {
      checkpoint: utils.timestampToOpId(row?.last_checkpoint ?? 0n),
      lsn: row?.last_checkpoint_lsn ?? null,
      hasSyncRules() {
        return row != null;
      },
      getBucketStorage: async () => {
        if (row == null) {
          return null;
        }
        return (await this.storageCache.fetch(Number(row.id))) ?? null;
      }
    } satisfies storage.ActiveCheckpoint;
  }
}
