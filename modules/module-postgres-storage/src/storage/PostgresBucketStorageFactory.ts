import { framework, GetIntanceOptions, storage, SyncRulesBucketStorage } from '@powersync/service-core';
import * as pg_wire from '@powersync/service-jpgwire';
import crypto from 'crypto';
import * as uuid from 'uuid';

import * as lib_postgres from '@powersync/lib-service-postgres';
import { models, NormalizedPostgresStorageConfig } from '../types/types.js';

import { getStorageApplicationName } from '../utils/application-name.js';
import { NOTIFICATION_CHANNEL, STORAGE_SCHEMA_NAME } from '../utils/db.js';
import { notifySyncRulesUpdate } from './batch/PostgresBucketBatch.js';
import { PostgresSyncRulesStorage } from './PostgresSyncRulesStorage.js';
import {
  PostgresPersistedReplicationStream,
  PostgresPersistedSyncConfigContent
} from './sync-rules/PostgresPersistedSyncConfigContent.js';

export type PostgresBucketStorageOptions = {
  config: NormalizedPostgresStorageConfig;
  slot_name_prefix: string;
};

export class PostgresBucketStorageFactory extends storage.BucketStorageFactory {
  [framework.DO_NOT_LOG] = true;
  readonly db: lib_postgres.DatabaseClient;
  public readonly slot_name_prefix: string;

  private activeStorageCache: storage.SyncRulesBucketStorage | undefined;

  constructor(protected options: PostgresBucketStorageOptions) {
    super();
    this.db = new lib_postgres.DatabaseClient({
      config: options.config,
      schema: STORAGE_SCHEMA_NAME,
      notificationChannels: [NOTIFICATION_CHANNEL],
      applicationName: getStorageApplicationName()
    });
    this.slot_name_prefix = options.slot_name_prefix;

    this.db.registerListener({
      connectionCreated: async (connection) => this.prepareStatements(connection)
    });
  }

  async [Symbol.asyncDispose]() {
    await this.db[Symbol.asyncDispose]();
  }

  async prepareStatements(connection: pg_wire.PgConnection) {
    // It should be possible to prepare statements for some common operations here.
    // This has not been implemented yet.
  }

  getInstance(
    replicationStream: storage.PersistedReplicationStream,
    options?: GetIntanceOptions
  ): storage.SyncRulesBucketStorage {
    const syncRuleStorage = new PostgresSyncRulesStorage({
      factory: this,
      db: this.db,
      replicationStream,
      batchLimits: this.options.config.batch_limits
    });
    if (!options?.skipLifecycleHooks) {
      this.iterateListeners((cb) => cb.syncStorageCreated?.(syncRuleStorage));
    }
    syncRuleStorage.registerListener({
      batchStarted: (batch) => {
        batch.registerListener({
          replicationEvent: (payload) => this.iterateListeners((cb) => cb.replicationEvent?.(payload))
        });
      }
    });
    return syncRuleStorage;
  }

  async getStorageMetrics(): Promise<storage.StorageMetrics> {
    const activeSyncConfigContent = await this.getActiveSyncConfigContent();
    if (activeSyncConfigContent == null) {
      return {
        operations_size_bytes: 0,
        parameters_size_bytes: 0,
        replication_size_bytes: 0
      };
    }

    const sizes = await this.db.sql`
      SELECT
        COALESCE(
          pg_total_relation_size(to_regclass('current_data')),
          0
        ) AS v1_current_size_bytes,
        COALESCE(
          pg_total_relation_size(to_regclass('v3_current_data')),
          0
        ) AS v3_current_size_bytes,
        pg_total_relation_size('bucket_parameters') AS parameter_size_bytes,
        pg_total_relation_size('bucket_data') AS operations_size_bytes;
    `.first<{
      v1_current_size_bytes: bigint;
      v3_current_size_bytes: bigint;
      parameter_size_bytes: bigint;
      operations_size_bytes: bigint;
    }>();

    return {
      operations_size_bytes: Number(sizes!.operations_size_bytes),
      parameters_size_bytes: Number(sizes!.parameter_size_bytes),
      replication_size_bytes: Number(sizes!.v1_current_size_bytes) + Number(sizes!.v3_current_size_bytes)
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

  async getSystemIdentifier(): Promise<storage.BucketStorageSystemIdentifier> {
    const id = lib_postgres.utils.encodePostgresSystemIdentifier(
      await lib_postgres.utils.queryPostgresSystemIdentifier(this.db.pool)
    );

    return {
      id,
      type: lib_postgres.POSTGRES_CONNECTION_TYPE
    };
  }

  async updateSyncRules(options: storage.UpdateSyncRulesOptions): Promise<PostgresPersistedReplicationStream> {
    const storageVersion =
      options.storageVersion ?? options.config.parsed.config.storageVersion ?? storage.CURRENT_STORAGE_VERSION;
    const storageConfig = storage.STORAGE_VERSION_CONFIG[storageVersion];
    if (storageConfig == null) {
      throw new framework.ServiceError(
        framework.ErrorCode.PSYNC_S1005,
        `Unsupported storage version ${storageVersion}`
      );
    }
    await this.initializeStorageVersion(storageConfig);
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
          sync_rules (
            id,
            content,
            sync_plan,
            state,
            slot_name,
            storage_version
          )
        VALUES
          (
            (
              SELECT
                id
              FROM
                next_id
            ),
            ${{ type: 'varchar', value: options.config.yaml }},
            ${{ type: 'json', value: options.config.plan }},
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
            ),
            ${{ type: 'int4', value: storageVersion }}
          )
        RETURNING
          *
      `
        .decoded(models.SyncRules)
        .first();

      await notifySyncRulesUpdate(this.db, newSyncRulesRow!);

      return new PostgresPersistedReplicationStream(this.db, newSyncRulesRow!);
    });
  }

  /**
   * Lazy-initializes storage-version-specific structures, if needed.
   */
  private async initializeStorageVersion(storageConfig: storage.StorageVersionConfig) {
    if (!storageConfig.softDeleteCurrentData) {
      return;
    }

    await this.db.sql`
      CREATE TABLE IF NOT EXISTS v3_current_data (
        group_id integer NOT NULL,
        source_table TEXT NOT NULL,
        source_key bytea NOT NULL,
        CONSTRAINT unique_v3_current_data_id PRIMARY KEY (group_id, source_table, source_key),
        buckets jsonb NOT NULL,
        data bytea NOT NULL,
        lookups bytea[] NOT NULL,
        pending_delete BIGINT NULL
      )
    `.execute();

    await this.db.sql`
      CREATE INDEX IF NOT EXISTS v3_current_data_pending_deletes ON v3_current_data (group_id, pending_delete)
      WHERE
        pending_delete IS NOT NULL
    `.execute();
  }

  async restartReplication(replicationStreamId: number): Promise<void> {
    const next = await this.getDeployingSyncConfigContent();
    const active = await this.getActiveSyncConfigContent();

    // In both the below cases, we create a new replication stream.
    // The current one will continue serving sync requests until the next one has finished processing.
    if (next != null && next.replicationStreamId == replicationStreamId) {
      // We need to redo the "next" replication stream

      await this.updateSyncRules(next.asUpdateOptions());
      // Pro-actively stop replicating
      await this.db.sql`
        UPDATE sync_rules
        SET
          state = ${{ value: storage.SyncRuleState.STOP, type: 'varchar' }}
        WHERE
          id = ${{ value: next.replicationStreamId, type: 'int4' }}
          AND state = ${{ value: storage.SyncRuleState.PROCESSING, type: 'varchar' }}
      `.execute();
    } else if (next == null && active?.replicationStreamId == replicationStreamId) {
      // Slot removed for "active" replication stream, while there is no "next" one.
      await this.updateSyncRules(active.asUpdateOptions());

      // Pro-actively stop replicating, but still serve clients with existing data
      await this.db.sql`
        UPDATE sync_rules
        SET
          state = ${{ value: storage.SyncRuleState.ERRORED, type: 'varchar' }}
        WHERE
          id = ${{ value: active.replicationStreamId, type: 'int4' }}
          AND state = ${{ value: storage.SyncRuleState.ACTIVE, type: 'varchar' }}
      `.execute();
    } else if (next != null && active?.replicationStreamId == replicationStreamId) {
      // Already have "next" replication stream - don't update any.

      // Pro-actively stop replicating, but still serve clients with existing data
      await this.db.sql`
        UPDATE sync_rules
        SET
          state = ${{ value: storage.SyncRuleState.ERRORED, type: 'varchar' }}
        WHERE
          id = ${{ value: active.replicationStreamId, type: 'int4' }}
          AND state = ${{ value: storage.SyncRuleState.ACTIVE, type: 'varchar' }}
      `.execute();
    }
  }

  async getActiveSyncConfigContent(): Promise<storage.PersistedSyncConfigContent | null> {
    const activeRow = await this.db.sql`
      SELECT
        *
      FROM
        sync_rules
      WHERE
        state = ${{ value: storage.SyncRuleState.ACTIVE, type: 'varchar' }}
        OR state = ${{ value: storage.SyncRuleState.ERRORED, type: 'varchar' }}
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

    return new PostgresPersistedSyncConfigContent(this.db, activeRow);
  }

  async getDeployingSyncConfigContent(): Promise<storage.PersistedSyncConfigContent | null> {
    const row = await this.db.sql`
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

    return row == null ? null : new PostgresPersistedSyncConfigContent(this.db, row);
  }

  async getReplicationStreamConfigs(replicationStreamId: number): Promise<storage.PersistedSyncConfigContent[]> {
    const row = await this.db.sql`
      SELECT
        *
      FROM
        sync_rules
      WHERE
        id = ${{ value: replicationStreamId, type: 'int4' }}
    `
      .decoded(models.SyncRules)
      .first();
    if (row == null) {
      return [];
    }

    return [new PostgresPersistedSyncConfigContent(this.db, row)];
  }

  async getSyncConfigContent(
    syncConfigId: storage.PersistedSyncConfigId
  ): Promise<storage.PersistedSyncConfigContent | null> {
    const replicationStreamId = Number(syncConfigId);
    if (!Number.isInteger(replicationStreamId)) {
      return null;
    }

    const row = await this.db.sql`
      SELECT
        *
      FROM
        sync_rules
      WHERE
        id = ${{ value: replicationStreamId, type: 'int4' }}
    `
      .decoded(models.SyncRules)
      .first();
    if (row == null) {
      return null;
    }

    return new PostgresPersistedSyncConfigContent(this.db, row);
  }

  async getReplicationStream(replicationStreamId: number): Promise<storage.PersistedReplicationStream | null> {
    const row = await this.db.sql`
      SELECT
        *
      FROM
        sync_rules
      WHERE
        id = ${{ value: replicationStreamId, type: 'int4' }}
    `
      .decoded(models.SyncRules)
      .first();
    if (row == null) {
      return null;
    }
    return new PostgresPersistedReplicationStream(this.db, row);
  }

  async getReplicatingReplicationStreams(): Promise<storage.PersistedReplicationStream[]> {
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

    return rows.map((row) => new PostgresPersistedReplicationStream(this.db, row));
  }

  async getStoppedReplicationStreams(): Promise<storage.PersistedReplicationStream[]> {
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

    return rows.map((row) => new PostgresPersistedReplicationStream(this.db, row));
  }

  async getActiveStorage(): Promise<SyncRulesBucketStorage | null> {
    const activeRow = await this.db.sql`
      SELECT
        *
      FROM
        sync_rules
      WHERE
        state = ${{ value: storage.SyncRuleState.ACTIVE, type: 'varchar' }}
        OR state = ${{ value: storage.SyncRuleState.ERRORED, type: 'varchar' }}
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
    const stream = new PostgresPersistedReplicationStream(this.db, activeRow);

    // It is important that this instance is cached.
    // Not for the instance construction itself, but to ensure that internal caches on the instance
    // are re-used properly.
    const activeStorageCache = this.activeStorageCache;
    if (activeStorageCache?.replicationStreamId == stream.replicationStreamId) {
      return activeStorageCache;
    } else {
      const instance = this.getInstance(stream);
      this.activeStorageCache = instance;
      return instance;
    }
  }
}
