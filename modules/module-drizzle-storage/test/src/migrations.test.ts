import { Direction } from '@powersync/lib-services-framework';
import { rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import {
  DRIZZLE_SQLITE_STORAGE_TYPE,
  DrizzleMigrationAgent,
  createSqliteDrizzleRuntime,
  normalizeDrizzleSqliteStorageConfig
} from '../../src/index.js';

describe('Drizzle migrations', () => {
  const files: string[] = [];
  const filename = () => {
    const file = join(tmpdir(), `powersync-drizzle-${process.pid}-${Date.now()}-${files.length}.sqlite`);
    files.push(file);
    return file;
  };

  afterEach(async () => {
    await Promise.all(
      files
        .splice(0)
        .flatMap((file) => [
          rm(file, { force: true }),
          rm(`${file}-shm`, { force: true }),
          rm(`${file}-wal`, { force: true })
        ])
    );
  });

  it('creates the storage tables and indexes', async () => {
    const file = filename();
    await using agent = new DrizzleMigrationAgent({ type: DRIZZLE_SQLITE_STORAGE_TYPE, filename: file });
    await agent.run({ direction: Direction.Up, migrations: [] });

    const runtime = createSqliteDrizzleRuntime(
      normalizeDrizzleSqliteStorageConfig({
        type: DRIZZLE_SQLITE_STORAGE_TYPE,
        filename: file
      })
    );
    try {
      const names = runtime.client
        .prepare(`SELECT name FROM sqlite_master WHERE type IN ('table', 'index')`)
        .all()
        .map((row: any) => row.name);
      expect(names).toEqual(
        expect.arrayContaining([
          'bucket_data',
          'bucket_parameters',
          'current_data',
          'op_id_sequence',
          'source_tables',
          'sync_rules',
          'write_checkpoints',
          'bucket_data_bucket_op_index',
          'bucket_parameters_lookup_index'
        ])
      );
      expect(runtime.client.prepare(`SELECT next_op_id FROM op_id_sequence WHERE id = 1`).get()).toEqual({
        next_op_id: 1n
      });
    } finally {
      runtime.close();
    }
  });

  it('configures WAL and separate readers for file storage', () => {
    const runtime = createSqliteDrizzleRuntime(
      normalizeDrizzleSqliteStorageConfig({
        type: DRIZZLE_SQLITE_STORAGE_TYPE,
        filename: filename(),
        max_pool_size: 3
      })
    );
    try {
      expect(runtime.readers).toHaveLength(2);
      expect(runtime.client.pragma('journal_mode', { simple: true })).toBe('wal');
    } finally {
      runtime.close();
    }
  });
});
