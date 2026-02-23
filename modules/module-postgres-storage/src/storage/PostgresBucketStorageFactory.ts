import * as framework from '@powersync/lib-services-framework';
import { GetIntanceOptions, storage, SyncRulesBucketStorage, UpdateSyncRulesOptions } from '@powersync/service-core';
import * as pg_wire from '@powersync/service-jpgwire';
import * as sync_rules from '@powersync/service-sync-rules';
import crypto from 'crypto';
import * as uuid from 'uuid';

import * as lib_postgres from '@powersync/lib-service-postgres';
import { models, NormalizedPostgresStorageConfig } from '../types/types.js';

import { NOTIFICATION_CHANNEL, STORAGE_SCHEMA_NAME } from '../utils/db.js';
import { notifySyncRulesUpdate } from './batch/PostgresBucketBatch.js';
import { PostgresSyncRulesStorage } from './PostgresSyncRulesStorage.js';
import { PostgresPersistedSyncRulesContent } from './sync-rules/PostgresPersistedSyncRulesContent.js';
import { getStorageApplicationName } from '../utils/application-name.js';

export type PostgresBucketStorageOptions = {
  config: NormalizedPostgresStorageConfig;
  slot_name_prefix: string;
};

export class PostgresBucketStorageFactory
  extends framework.BaseObserver<storage.BucketStorageFactoryListener>
  implements storage.BucketStorageFactory
{
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
    syncRules: storage.PersistedSyncRulesContent,
    options?: GetIntanceOptions
  ): storage.SyncRulesBucketStorage {
    const storage = new PostgresSyncRulesStorage({
      factory: this,
      db: this.db,
      sync_rules: syncRules,
      batchLimits: this.options.config.batch_limits
    });
    if (!options?.skipLifecycleHooks) {
      this.iterateListeners((cb) => cb.syncStorageCreated?.(storage));
    }
    storage.registerListener({
      batchStarted: (batch) => {
        batch.registerListener({
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

  // TODO possibly share implementation in abstract class
  async configureSyncRules(options: UpdateSyncRulesOptions): Promise<{
    updated: boolean;
    persisted_sync_rules?: storage.PersistedSyncRulesContent;
    lock?: storage.ReplicationLock;
  }> {
    const next = await this.getNextSyncRulesContent();
    const active = await this.getActiveSyncRulesContent();

    if (next?.sync_rules_content == options.content) {
      framework.logger.info('Sync rules from configuration unchanged');
      return { updated: false };
    } else if (next == null && active?.sync_rules_content == options.content) {
      framework.logger.info('Sync rules from configuration unchanged');
      return { updated: false };
    } else {
      framework.logger.info('Sync rules updated from configuration');
      const persisted_sync_rules = await this.updateSyncRules(options);
      return { updated: true, persisted_sync_rules, lock: persisted_sync_rules.current_lock ?? undefined };
    }
  }

  async updateSyncRules(options: storage.UpdateSyncRulesOptions): Promise<PostgresPersistedSyncRulesContent> {
    // TODO some shared implementation for this might be nice
    if (options.validate) {
      // Parse and validate before applying any changes
      sync_rules.SqlSyncRules.fromYaml(options.content, {
        // No schema-based validation at this point
        schema: undefined,
        defaultSchema: 'not_applicable', // Not needed for validation
        throwOnError: true
      });
    } else {
      // Apply unconditionally. Any errors will be reported via the diagnostics API.
    }

    const storageVersion = options.storageVersion ?? storage.CURRENT_STORAGE_VERSION;
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
          sync_rules (id, content, state, slot_name, storage_version)
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
            ),
            ${{ type: 'int4', value: storageVersion }}
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

  async restartReplication(sync_rules_group_id: number): Promise<void> {
    const next = await this.getNextSyncRulesContent();
    const active = await this.getActiveSyncRulesContent();

    // In both the below cases, we create a new sync rules instance.
    // The current one will continue serving sync requests until the next one has finished processing.
    if (next != null && next.id == sync_rules_group_id) {
      // We need to redo the "next" sync rules
      await this.updateSyncRules({
        content: next.sync_rules_content,
        validate: false
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
    } else if (next == null && active?.id == sync_rules_group_id) {
      // Slot removed for "active" sync rules, while there is no "next" one.
      await this.updateSyncRules({
        content: active.sync_rules_content,
        validate: false
      });

      // Pro-actively stop replicating, but still serve clients with existing data
      await this.db.sql`
        UPDATE sync_rules
        SET
          state = ${{ value: storage.SyncRuleState.ERRORED, type: 'varchar' }}
        WHERE
          id = ${{ value: active.id, type: 'int4' }}
          AND state = ${{ value: storage.SyncRuleState.ACTIVE, type: 'varchar' }}
      `.execute();
    } else if (next != null && active?.id == sync_rules_group_id) {
      // Already have "next" sync rules - don't update any.

      // Pro-actively stop replicating, but still serve clients with existing data
      await this.db.sql`
        UPDATE sync_rules
        SET
          state = ${{ value: storage.SyncRuleState.ERRORED, type: 'varchar' }}
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

  async getActiveStorage(): Promise<SyncRulesBucketStorage | null> {
    const content = await this.getActiveSyncRulesContent();
    if (content == null) {
      return null;
    }

    // It is important that this instance is cached.
    // Not for the instance construction itself, but to ensure that internal caches on the instance
    // are re-used properly.
    if (this.activeStorageCache?.group_id == content.id) {
      return this.activeStorageCache;
    } else {
      const instance = this.getInstance(content);
      this.activeStorageCache = instance;
      return instance;
    }
  }
}
