import { InProcessDrizzleCheckpointWatcher, type DrizzleStorageDialect } from '../../storage/DrizzleStorageDialect.js';
import { DRIZZLE_SQLITE_STORAGE_TYPE } from '../../types/types.js';
import { sqliteSchema } from './schema.js';
import type { SqliteDrizzleRuntime } from './sqlite-config.js';

export function createSqliteDrizzleStorageDialect(runtime: SqliteDrizzleRuntime): DrizzleStorageDialect {
  return {
    type: DRIZZLE_SQLITE_STORAGE_TYPE,
    db: runtime.db,
    tables: sqliteSchema,
    transaction: (callback) => runtime.transaction(callback),
    async *streamBucketDataRows(options) {
      if (options.dataBuckets.length == 0) {
        return;
      }

      await new Promise<void>((resolve) => setImmediate(resolve));
      const db = options.db ?? runtime.read();
      const sortedBuckets = [...options.dataBuckets].sort((a, b) => a.bucket.localeCompare(b.bucket));
      let remaining = options.limit;
      const statement = db.$client
        .prepare(
          `
        SELECT bucket_name, op_id, op, source_table, source_key,
               table_name, row_id, checksum, data, target_op
        FROM bucket_data
        WHERE group_id = ? AND bucket_name = ? AND op_id > ? AND op_id <= ?
        ORDER BY op_id ASC
        LIMIT ?
      `
        )
        .safeIntegers(true);
      for (const request of sortedBuckets) {
        if (remaining <= 0) {
          return;
        }
        const rows = statement.iterate(
          options.groupId,
          request.bucket,
          request.start,
          options.checkpoint,
          remaining
        ) as Iterable<RawBucketDataRow>;
        for (const row of rows) {
          yield mapBucketDataRow(row);
          remaining--;
        }
      }
    },
    createCheckpointWatcher: () => new InProcessDrizzleCheckpointWatcher()
  };
}

interface RawBucketDataRow {
  bucket_name: string;
  op_id: bigint;
  op: string;
  source_table: string | null;
  source_key: Buffer | null;
  table_name: string | null;
  row_id: string | null;
  checksum: bigint;
  data: string | null;
  target_op: bigint | null;
}

function mapBucketDataRow(row: RawBucketDataRow) {
  return {
    bucketName: row.bucket_name,
    opId: row.op_id,
    op: row.op,
    sourceTable: row.source_table,
    sourceKey: row.source_key,
    tableName: row.table_name,
    rowId: row.row_id,
    checksum: row.checksum,
    data: row.data,
    targetOp: row.target_op
  };
}
