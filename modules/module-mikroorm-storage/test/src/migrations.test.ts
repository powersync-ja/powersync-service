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
    await Promise.all(
      dbFiles.splice(0).flatMap((file) => [
        rm(file, { force: true }),
        rm(`${file}-shm`, { force: true }),
        rm(`${file}-wal`, { force: true })
      ])
    );
  });

  it('enables WAL and read replicas for file-backed SQLite storage', async () => {
    const filename = createDbFile();
    const orm = await MikroORM.init<SqliteDriver>(
      createSqliteMikroOrmOptions(
        normalizeMikroOrmSqliteStorageConfig({
          type: MIKRO_ORM_SQLITE_STORAGE_TYPE,
          filename,
          max_pool_size: 3
        })
      )
    );

    try {
      const driver = orm.em.getDriver();
      const writeConnection = driver.getConnection('write');
      const readConnection = driver.getConnection('read');

      expect(readConnection).not.toBe(writeConnection);
      await expect(writeConnection.execute<{ journal_mode: string }[]>('PRAGMA journal_mode')).resolves.toEqual([
        { journal_mode: 'wal' }
      ]);
      await expect(readConnection.execute<{ journal_mode: string }[]>('PRAGMA journal_mode')).resolves.toEqual([
        { journal_mode: 'wal' }
      ]);
    } finally {
      await orm.close(true);
    }
  });

  it('reads from a replica while the write connection has an open transaction', async () => {
    const filename = createDbFile();
    const orm = await MikroORM.init<SqliteDriver>(
      createSqliteMikroOrmOptions(
        normalizeMikroOrmSqliteStorageConfig({
          type: MIKRO_ORM_SQLITE_STORAGE_TYPE,
          filename,
          max_pool_size: 2
        })
      )
    );

    try {
      await orm.em.execute('create table concurrency_test (id integer primary key, value text)', [], 'run');
      await orm.em.execute(`insert into concurrency_test (id, value) values (1, 'committed')`, [], 'run');

      let releaseWriter!: () => void;
      const releaseWriterPromise = new Promise<void>((resolve) => {
        releaseWriter = resolve;
      });
      let writerReady!: () => void;
      const writerReadyPromise = new Promise<void>((resolve) => {
        writerReady = resolve;
      });

      const writer = orm.em.fork().transactional(async (transactionalEntityManager) => {
        await transactionalEntityManager.execute(
          `update concurrency_test set value = 'uncommitted' where id = 1`,
          [],
          'run'
        );
        writerReady();
        await releaseWriterPromise;
      });

      await writerReadyPromise;
      try {
        const start = performance.now();
        const rows = await orm.em
          .fork()
          .getKysely<{ concurrency_test: { id: number; value: string } }>({ type: 'read' })
          .selectFrom('concurrency_test')
          .select('value')
          .where('id', '=', 1)
          .execute();
        const elapsed = performance.now() - start;

        expect(rows).toEqual([{ value: 'committed' }]);
        expect(elapsed).toBeLessThan(1_000);
      } finally {
        releaseWriter();
        await writer;
      }
    } finally {
      await orm.close(true);
    }
  });

  it('does not allow overlapping write transactions', async () => {
    const filename = createDbFile();
    const orm = await MikroORM.init<SqliteDriver>(
      createSqliteMikroOrmOptions(
        normalizeMikroOrmSqliteStorageConfig({
          type: MIKRO_ORM_SQLITE_STORAGE_TYPE,
          filename,
          max_pool_size: 2
        })
      )
    );

    try {
      await orm.em.execute('create table write_overlap_test (id integer primary key, value text)', [], 'run');
      await orm.em.execute(`insert into write_overlap_test (id, value) values (1, 'first')`, [], 'run');

      let releaseFirstWriter!: () => void;
      const releaseFirstWriterPromise = new Promise<void>((resolve) => {
        releaseFirstWriter = resolve;
      });
      let firstWriterReady!: () => void;
      const firstWriterReadyPromise = new Promise<void>((resolve) => {
        firstWriterReady = resolve;
      });

      const firstWriter = orm.em.fork().transactional(async (transactionalEntityManager) => {
        await transactionalEntityManager.execute(`update write_overlap_test set value = 'held' where id = 1`, [], 'run');
        firstWriterReady();
        await releaseFirstWriterPromise;
      });

      await firstWriterReadyPromise;

      let secondWriterCompletedBeforeRelease = false;
      const secondWriter = orm.em
        .fork()
        .transactional(async (transactionalEntityManager) => {
          await transactionalEntityManager.execute(`update write_overlap_test set value = 'second' where id = 1`, [], 'run');
        })
        .then(
          () => ({ status: 'fulfilled' as const }),
          (error) => ({ status: 'rejected' as const, error })
        );

      const secondWriterState = await Promise.race([
        secondWriter.then((result) => {
          secondWriterCompletedBeforeRelease = result.status == 'fulfilled';
          return result;
        }),
        new Promise<{ status: 'pending' }>((resolve) => setTimeout(() => resolve({ status: 'pending' }), 50))
      ]);

      expect(secondWriterCompletedBeforeRelease).toBe(false);
      const valueBeforeRelease = await orm.em
        .fork()
        .getKysely<{ write_overlap_test: { id: number; value: string } }>({ type: 'read' })
        .selectFrom('write_overlap_test')
        .select('value')
        .where('id', '=', 1)
        .execute();
      expect(valueBeforeRelease).toEqual([{ value: 'first' }]);

      releaseFirstWriter();
      await firstWriter;

      if (secondWriterState.status == 'pending') {
        await expect(secondWriter).resolves.toEqual({ status: 'fulfilled' });
      } else {
        expect(secondWriterState.status).toBe('rejected');
        await orm.em
          .fork()
          .transactional(async (transactionalEntityManager) => {
            await transactionalEntityManager.execute(
              `update write_overlap_test set value = 'second' where id = 1`,
              [],
              'run'
            );
          });
      }

      const finalValue = await orm.em.execute<{ value: string }[]>(
        'select value from write_overlap_test where id = 1'
      );
      expect(finalValue).toEqual([{ value: 'second' }]);
    } finally {
      await orm.close(true);
    }
  });

  it('keeps in-memory SQLite storage on a single connection', async () => {
    const orm = await MikroORM.init<SqliteDriver>(
      createSqliteMikroOrmOptions(
        normalizeMikroOrmSqliteStorageConfig({
          type: MIKRO_ORM_SQLITE_STORAGE_TYPE,
          filename: ':memory:',
          max_pool_size: 3
        })
      )
    );

    try {
      const driver = orm.em.getDriver();
      expect(driver.getConnection('read')).toBe(driver.getConnection('write'));
    } finally {
      await orm.close(true);
    }
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
