import { migrate } from 'drizzle-orm/better-sqlite3/migrator';
import { describe, expect, it } from 'vitest';
import {
  createSqliteDrizzleRuntime,
  DRIZZLE_SQLITE_STORAGE_TYPE,
  normalizeDrizzleSqliteStorageConfig,
  SQLITE_DRIZZLE_MIGRATIONS_PATH,
  sqliteSchema
} from '../../src/index.js';

describe('Drizzle SQLite column mappings', () => {
  it('round-trips runtime values without bigint precision loss', () => {
    const runtime = createSqliteDrizzleRuntime(
      normalizeDrizzleSqliteStorageConfig({
        type: DRIZZLE_SQLITE_STORAGE_TYPE,
        filename: ':memory:'
      })
    );
    try {
      migrate(runtime.db, { migrationsFolder: SQLITE_DRIZZLE_MIGRATIONS_PATH });
      const opId = BigInt(Number.MAX_SAFE_INTEGER) + 100n;
      runtime.db
        .insert(sqliteSchema.bucketData)
        .values({
          id: '1',
          groupId: 1,
          bucketName: 'bucket',
          opId,
          op: 'PUT',
          checksum: 2n,
          sourceTable: 'table',
          sourceKey: Buffer.from('key'),
          tableName: 'items',
          rowId: 'row',
          data: '{}',
          targetOp: null
        })
        .run();
      const row = runtime.db.select().from(sqliteSchema.bucketData).get()!;
      expect(row.opId).toBe(opId);
      expect(row.checksum).toBe(2n);
      expect(row.sourceKey).toEqual(Buffer.from('key'));
    } finally {
      runtime.close();
    }
  });
});
