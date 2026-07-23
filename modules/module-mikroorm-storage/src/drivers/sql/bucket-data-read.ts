import type { SqlEntityManager } from '@mikro-orm/sql';
import type { BucketData } from '../../entities/entities-index.js';
import type { MikroOrmBucketDataStreamOptions } from '../../storage/MikroOrmStorageDialect.js';

const REQUEST_BUCKET_CHUNK_SIZE = 400;

export interface SqlBucketDataReadQueryOptions extends MikroOrmBucketDataStreamOptions {
  readonly limit: number;
}

export interface SqlBucketDataReadQuery {
  readonly sql: string;
  readonly params: unknown[];
}

export async function* streamBucketDataRowsFromSql(
  options: MikroOrmBucketDataStreamOptions,
  buildQuery: (options: SqlBucketDataReadQueryOptions) => SqlBucketDataReadQuery
): AsyncIterable<BucketData> {
  const sqlEntityManager = options.em as SqlEntityManager;
  const sortedBuckets = [...options.dataBuckets].sort((a, b) => a.bucket.localeCompare(b.bucket));
  let remainingLimit = options.limit;

  for (let offset = 0; offset < sortedBuckets.length && remainingLimit > 0; offset += REQUEST_BUCKET_CHUNK_SIZE) {
    const dataBuckets = sortedBuckets.slice(offset, offset + REQUEST_BUCKET_CHUNK_SIZE);
    const query = buildQuery({
      ...options,
      dataBuckets,
      limit: remainingLimit
    });
    const rows = await sqlEntityManager.getConnection().execute<RawBucketDataRow[]>(query.sql, query.params, 'all');

    for (const row of rows) {
      yield rawBucketDataRow(row);
    }

    remainingLimit -= rows.length;
  }
}

interface RawBucketDataRow {
  id: string;
  group_id: number;
  bucket_name: string;
  op_id: bigint | number | string;
  op: string;
  source_table: string | null;
  source_key: Uint8Array | null;
  table_name: string | null;
  row_id: string | null;
  checksum: bigint | number | string;
  data: string | null;
  target_op: bigint | number | string | null;
}

function rawBucketDataRow(row: RawBucketDataRow): BucketData {
  return {
    id: row.id,
    groupId: row.group_id,
    bucketName: row.bucket_name,
    opId: BigInt(row.op_id),
    op: row.op,
    sourceTable: row.source_table,
    sourceKey: row.source_key,
    tableName: row.table_name,
    rowId: row.row_id,
    checksum: BigInt(row.checksum),
    data: row.data,
    targetOp: row.target_op == null ? null : BigInt(row.target_op)
  };
}
