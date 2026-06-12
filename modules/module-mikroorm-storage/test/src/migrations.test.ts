import { rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { MikroORM } from '@mikro-orm/core';
import { SqliteDriver } from '@mikro-orm/sqlite';
import { Direction } from '@powersync/lib-services-framework';
import { storage } from '@powersync/service-core';
import { afterEach, describe, expect, it } from 'vitest';
import { createSqliteMikroOrmOptions } from '../../src/drivers/sqlite/sqlite-config.js';
import { sqliteMikroOrmStorageDialect } from '../../src/drivers/sqlite/sqlite-dialect.js';
import { MIKRO_ORM_SQLITE_STORAGE_TYPE } from '../../src/index.js';
import { MikroOrmMigrationAgent } from '../../src/migrations/MikroOrmMigrationAgent.js';
import { normalizeMikroOrmSqliteStorageConfig } from '../../src/types/types.js';

describe('MikroORM migrations', () => {
  const dbFiles: string[] = [];

  const createDbFile = () => {
    const filename = join(tmpdir(), `powersync-mikroorm-storage-${process.pid}-${Date.now()}-${dbFiles.length}.sqlite`);
    dbFiles.push(filename);
    return filename;
  };

  afterEach(async () => {
    await Promise.all(dbFiles.splice(0).map((file) => rm(file, { force: true })));
  });

  it('runs SQLite migrations through the service migration agent', async () => {
    const filename = createDbFile();

    await using agent = new MikroOrmMigrationAgent({
      type: MIKRO_ORM_SQLITE_STORAGE_TYPE,
      filename
    });

    await agent.run({
      direction: Direction.Up,
      migrations: []
    });

    const orm = await MikroORM.init<SqliteDriver>(
      createSqliteMikroOrmOptions(
        normalizeMikroOrmSqliteStorageConfig({
          type: MIKRO_ORM_SQLITE_STORAGE_TYPE,
          filename
        })
      )
    );

    try {
      const tables = await orm.em
        .getConnection()
        .execute<{ name: string }[]>(`select name from sqlite_master where type = 'table' order by name`);
      const tableNames = tables.map((row) => row.name);

      expect(tableNames).toContain('powersync_migration_locks');
      expect(tableNames).toContain('mikro_orm_migrations');
      expect(tableNames).toContain('instance');
      expect(tableNames).toContain('sync_rules');
      expect(tableNames).toContain('bucket_data');
      expect(tableNames).toContain('write_checkpoints');

      const indexes = await orm.em
        .getConnection()
        .execute<{ name: string }[]>(`select name from sqlite_master where type = 'index' order by name`);
      const indexNames = indexes.map((row) => row.name);

      expect(indexNames).toContain('bucket_data_bucket_op_index');
      expect(indexNames).toContain('bucket_parameters_lookup_index');
      expect(indexNames).toContain('bucket_parameters_source_index');
      expect(indexNames).toContain('current_data_pending_delete_index');
      expect(indexNames).toContain('source_table_lookup');
      expect(indexNames).toContain('write_checkpoints_user_checkpoint_index');
    } finally {
      await orm.close(true);
    }
  });

  it('hydrates SQLite-specific storage columns as service-facing values', async () => {
    const filename = createDbFile();

    await using agent = new MikroOrmMigrationAgent({
      type: MIKRO_ORM_SQLITE_STORAGE_TYPE,
      filename
    });

    await agent.run({
      direction: Direction.Up,
      migrations: []
    });

    const orm = await MikroORM.init<SqliteDriver>(
      createSqliteMikroOrmOptions(
        normalizeMikroOrmSqliteStorageConfig({
          type: MIKRO_ORM_SQLITE_STORAGE_TYPE,
          filename
        })
      )
    );

    try {
      const em = orm.em.fork();
      const bucketRow = em.create(sqliteMikroOrmStorageDialect.bucketDataEntity, {
        id: 'test-bucket-row',
        groupId: 1,
        bucketName: 'bucket[]',
        opId: 42n,
        op: 'PUT',
        sourceTable: null,
        sourceKey: Buffer.from('source-key'),
        tableName: null,
        rowId: null,
        checksum: 99n,
        data: null,
        targetOp: null
      });

      const syncPlan: storage.SerializedSyncPlan = {
        plan: {
          version: 1,
          dataSources: [],
          buckets: [],
          parameterIndexes: [],
          streams: []
        },
        compatibility: {
          edition: 1,
          overrides: {}
        },
        eventDescriptors: {}
      };

      const syncRulesRow = em.create(sqliteMikroOrmStorageDialect.syncRulesEntity, {
        state: storage.SyncRuleState.PROCESSING,
        snapshotDone: false,
        snapshotLsn: null,
        lastCheckpoint: 123n,
        lastCheckpointLsn: null,
        noCheckpointBefore: null,
        slotName: 'test_slot',
        lastCheckpointTs: null,
        lastKeepaliveTs: null,
        lastFatalError: null,
        lastFatalErrorTs: null,
        keepaliveOp: 124n,
        storageVersion: storage.CURRENT_STORAGE_VERSION,
        content: 'bucket_definitions: []',
        syncPlan
      });

      em.persist([bucketRow, syncRulesRow]);
      await em.flush();
      em.clear();

      const storedBucketRow = await em.findOneOrFail(sqliteMikroOrmStorageDialect.bucketDataEntity, {
        id: 'test-bucket-row'
      });
      const storedSyncRulesRow = await em.findOneOrFail(sqliteMikroOrmStorageDialect.syncRulesEntity, {
        id: syncRulesRow.id
      });

      expect(storedBucketRow.opId).toBe(42n);
      expect(storedBucketRow.checksum).toBe(99n);
      expect(storedBucketRow.sourceKey).toBeInstanceOf(Buffer);
      expect(storedBucketRow.sourceKey?.toString()).toBe('source-key');
      expect(storedSyncRulesRow.lastCheckpoint).toBe(123n);
      expect(storedSyncRulesRow.keepaliveOp).toBe(124n);
      expect(storedSyncRulesRow.syncPlan).toEqual(syncPlan);
    } finally {
      await orm.close(true);
    }
  });
});
