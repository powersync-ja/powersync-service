import type { MikroORM } from '@mikro-orm/core';
import { Logger } from '@powersync/lib-services-framework';
import { storage, utils } from '@powersync/service-core';
import * as uuid from 'uuid';
import type { MikroOrmStorageDialect } from './MikroOrmStorageDialect.js';

interface CurrentBucketState {
  bucket: string;
  seen: Map<string, bigint>;
  trackingSize: number;
  lastNotPut: bigint | null;
  opsSincePut: number;
}

interface ParameterCompactionRow {
  id: bigint;
  sourceTable: string;
  sourceKey: Buffer;
  lookup: Buffer;
  bucketParameters: unknown;
}

interface RawParameterCompactionRow {
  id: bigint | number | string;
  source_table?: string;
  sourceTable?: string;
  source_key?: Buffer | Uint8Array;
  sourceKey?: Buffer | Uint8Array;
  lookup: Buffer | Uint8Array;
  bucket_parameters?: unknown;
  bucketParameters?: unknown;
}

interface RawBucketDataRow {
  id: string;
  bucket_name: string;
  op_id: bigint | number | string;
  op: string;
  source_table: string | null;
  source_key: Buffer | Uint8Array | null;
  table_name: string | null;
  row_id: string | null;
  checksum: bigint | number | string;
  target_op: bigint | number | string | null;
}

interface CompactionBucketDataRow {
  id: string;
  bucketName: string;
  opId: bigint;
  op: string;
  sourceTable: string | null;
  sourceKey: Buffer | null;
  tableName: string | null;
  rowId: string | null;
  checksum: bigint;
  targetOp: bigint | null;
}

export interface MikroOrmCompactOptions extends storage.CompactOptions {
  logger: Logger;
}

const BIGINT_MAX = 9223372036854775807n;
const DEFAULT_CLEAR_BATCH_LIMIT = 5000;
const DEFAULT_MOVE_BATCH_LIMIT = 2000;
const DEFAULT_MOVE_BATCH_QUERY_LIMIT = 10_000;
const DEFAULT_MEMORY_LIMIT_MB = 64;
const PARAMETER_DELETE_BATCH_LIMIT = 1000;
const PARAMETER_SCAN_BATCH_LIMIT = 10_000;

export class MikroOrmCompactor {
  private readonly idLimitBytes: number;
  private readonly moveBatchLimit: number;
  private readonly moveBatchQueryLimit: number;
  private readonly clearBatchLimit: number;
  private readonly maxOpId: bigint;
  private readonly buckets: string[] | undefined;
  private readonly logger: Logger;

  private pendingMoves: { id: string; targetOp: bigint }[] = [];

  constructor(
    private readonly orm: MikroORM,
    private readonly dialect: MikroOrmStorageDialect,
    private readonly groupId: number,
    options: MikroOrmCompactOptions
  ) {
    this.idLimitBytes = (options.memoryLimitMB ?? DEFAULT_MEMORY_LIMIT_MB) * 1024 * 1024;
    this.moveBatchLimit = options.moveBatchLimit ?? DEFAULT_MOVE_BATCH_LIMIT;
    this.moveBatchQueryLimit = options.moveBatchQueryLimit ?? DEFAULT_MOVE_BATCH_QUERY_LIMIT;
    this.clearBatchLimit = options.clearBatchLimit ?? DEFAULT_CLEAR_BATCH_LIMIT;
    this.maxOpId = options.maxOpId ?? 0n;
    this.buckets = options.compactBuckets;
    this.logger = options.logger;
  }

  async compact(): Promise<void> {
    if (this.maxOpId <= 0n) {
      return;
    }

    if (this.buckets != null) {
      for (const bucket of this.buckets) {
        await this.compactSingleBucket(bucket);
      }
    } else {
      await this.compactAllBuckets();
    }
  }

  async compactParameterData(options: storage.CompactOptions): Promise<void> {
    if (this.maxOpId <= 0n) {
      return;
    }

    const lastByKey = new Map<string, ParameterCompactionRow>();
    const removeIds = new Set<bigint>();
    const keysWithDuplicateValues = new Set<string>();
    const maxCacheSize = options.compactParameterCacheLimit ?? 10_000;
    const flushDeletes = async (force: boolean) => {
      if (removeIds.size < PARAMETER_DELETE_BATCH_LIMIT && !(force && removeIds.size > 0)) {
        return;
      }

      const removedCount = removeIds.size;
      for (const id of removeIds) {
        await this.executeRun(
          `
          DELETE FROM bucket_parameters
          WHERE group_id = ? AND id = ?
          `,
          [this.groupId, id.toString()]
        );
      }
      removeIds.clear();
      this.logger.info(`Removed ${removedCount} compacted parameter entries`);
    };

    for await (const row of this.streamParameterRows()) {
      const key = parameterKey(row);
      const previous = lastByKey.get(key);
      if (previous != null && sameBucketParameters(previous.bucketParameters, row.bucketParameters)) {
        removeIds.add(row.id);
        keysWithDuplicateValues.add(key);
      }

      if (isEmptyBucketParameters(row.bucketParameters) && row.id < this.maxOpId && !keysWithDuplicateValues.has(key)) {
        await flushDeletes(true);
        const candidate = lastByKey.get(key);
        await this.executeRun(
          `
          DELETE FROM bucket_parameters
          WHERE group_id = ?
            AND lookup = ?
            AND source_table = ?
            AND source_key = ?
            AND id <= ?
          `,
          [this.groupId, row.lookup, row.sourceTable, row.sourceKey, row.id.toString()]
        );
        if (candidate != null && candidate.id <= row.id) {
          removeIds.add(candidate.id);
        }
        removeIds.add(row.id);
        lastByKey.delete(key);
      } else {
        lastByKey.set(key, row);
      }

      if (lastByKey.size > maxCacheSize) {
        const oldest = lastByKey.keys().next().value;
        if (oldest != null) {
          lastByKey.delete(oldest);
        }
      }

      await flushDeletes(false);
    }
    await flushDeletes(true);
    lastByKey.clear();
  }

  private async *streamParameterRows(): AsyncIterable<ParameterCompactionRow> {
    let lastRow: ParameterCompactionRow | null = null;

    while (true) {
      const params: unknown[] = [this.groupId, this.maxOpId];
      let cursorFilter = '';
      if (lastRow != null) {
        cursorFilter = `
          AND (
            lookup > ?
            OR (lookup = ? AND source_table > ?)
            OR (lookup = ? AND source_table = ? AND source_key > ?)
            OR (lookup = ? AND source_table = ? AND source_key = ? AND id > ?)
          )
        `;
        params.push(
          lastRow.lookup,
          lastRow.lookup,
          lastRow.sourceTable,
          lastRow.lookup,
          lastRow.sourceTable,
          lastRow.sourceKey,
          lastRow.lookup,
          lastRow.sourceTable,
          lastRow.sourceKey,
          lastRow.id
        );
      }
      params.push(PARAMETER_SCAN_BATCH_LIMIT);

      const rows = await this.executeAll<RawParameterCompactionRow>(
        `
        SELECT id, source_table, source_key, lookup, bucket_parameters
        FROM bucket_parameters
        WHERE group_id = ?
          AND id <= ?
          ${cursorFilter}
        ORDER BY lookup ASC, source_table ASC, source_key ASC, id ASC
        LIMIT ?
        `,
        params
      );

      if (rows.length == 0) {
        return;
      }

      for (const rawRow of rows) {
        lastRow = rawParameterRow(rawRow);
        yield lastRow;
      }
    }
  }

  private async compactAllBuckets(): Promise<void> {
    const discoveryBatchSize = 200;
    let lastBucket = '';

    while (true) {
      const rows = await this.executeAll<{ bucket_name: string }>(
        `
        SELECT DISTINCT bucket_name
        FROM bucket_data
        WHERE group_id = ? AND bucket_name > ?
        ORDER BY bucket_name ASC
        LIMIT ?
        `,
        [this.groupId, lastBucket, discoveryBatchSize]
      );
      if (rows.length == 0) {
        break;
      }

      for (const row of rows) {
        await this.compactSingleBucket(row.bucket_name);
      }
      lastBucket = rows[rows.length - 1].bucket_name;
    }
  }

  private async compactSingleBucket(bucket: string): Promise<void> {
    const currentState: CurrentBucketState = {
      bucket,
      seen: new Map(),
      trackingSize: 0,
      lastNotPut: null,
      opsSincePut: 0
    };
    let upperOpIdLimit = BIGINT_MAX;

    while (true) {
      const batch = (
        await this.executeAll<RawBucketDataRow>(
          `
          SELECT id, bucket_name, op_id, op, source_table, source_key, table_name, row_id, checksum, target_op
          FROM bucket_data
          WHERE group_id = ?
            AND bucket_name = ?
            AND op_id < ?
            AND op_id <= ?
          ORDER BY op_id DESC
          LIMIT ?
          `,
          [this.groupId, bucket, upperOpIdLimit, this.maxOpId, this.moveBatchQueryLimit]
        )
      ).map(rawBucketDataRow);

      if (batch.length == 0) {
        break;
      }

      upperOpIdLimit = batch[batch.length - 1].opId;

      for (const row of batch) {
        let isPersistentPut = row.op == 'PUT';

        if (row.op == 'REMOVE' || row.op == 'PUT') {
          const key = compactBucketRowKey(row);
          const targetOp = currentState.seen.get(utils.flatstr(key));
          if (targetOp != null) {
            isPersistentPut = false;
            this.pendingMoves.push({ id: row.id, targetOp });
          } else if (currentState.trackingSize < this.idLimitBytes) {
            currentState.seen.set(utils.flatstr(key), row.opId);
            currentState.trackingSize += key.length + 140;
          }
        }

        if (isPersistentPut) {
          currentState.lastNotPut = null;
          currentState.opsSincePut = 0;
        } else if (row.op != 'CLEAR') {
          currentState.lastNotPut ??= row.opId;
          currentState.opsSincePut += 1;
        }

        if (this.pendingMoves.length >= this.moveBatchLimit) {
          await this.flushMoves();
        }
      }
    }

    await this.flushMoves();
    currentState.seen.clear();
    if (currentState.lastNotPut != null && currentState.opsSincePut > 1) {
      this.logger.info(
        `Inserting CLEAR at ${this.groupId}:${currentState.bucket}:${currentState.lastNotPut} to remove ${currentState.opsSincePut} operations`
      );
      await this.clearBucket(currentState.bucket, currentState.lastNotPut);
    }
  }

  private async flushMoves(): Promise<void> {
    if (this.pendingMoves.length == 0) {
      return;
    }

    const batch = this.pendingMoves.splice(0);
    this.logger.info(`Compacting ${batch.length} ops`);
    for (const { id, targetOp } of batch) {
      await this.executeRun(
        `
        UPDATE bucket_data
        SET op = 'MOVE',
            target_op = ?,
            table_name = NULL,
            row_id = NULL,
            data = NULL,
            source_table = NULL,
            source_key = NULL
        WHERE id = ?
        `,
        [targetOp, id]
      );
    }
  }

  private async clearBucket(bucket: string, op: bigint): Promise<void> {
    let done = false;
    while (!done) {
      const operationRows = (
        await this.executeAll<RawBucketDataRow>(
          `
          SELECT id, bucket_name, op_id, op, source_table, source_key, table_name, row_id, checksum, target_op
          FROM bucket_data
          WHERE group_id = ?
            AND bucket_name = ?
            AND op_id <= ?
          ORDER BY op_id ASC
          LIMIT ?
          `,
          [this.groupId, bucket, op, this.clearBatchLimit]
        )
      ).map(rawBucketDataRow);

      let checksum = 0;
      let lastOpId: bigint | null = null;
      let targetOp: bigint | null = null;
      let gotAnOp = false;

      for (const operation of operationRows) {
        if (operation.op != 'MOVE' && operation.op != 'REMOVE' && operation.op != 'CLEAR') {
          throw new Error(`Unexpected ${operation.op} operation at ${this.groupId}:${bucket}:${operation.opId}`);
        }

        checksum = utils.addChecksums(checksum, Number(operation.checksum));
        lastOpId = operation.opId;
        if (operation.op != 'CLEAR') {
          gotAnOp = true;
        }
        if (operation.targetOp != null && (targetOp == null || operation.targetOp > targetOp)) {
          targetOp = operation.targetOp;
        }
      }

      if (!gotAnOp || lastOpId == null) {
        done = true;
        return;
      }

      this.logger.info(`Flushing CLEAR at ${lastOpId}`);
      await this.executeRun(
        `
        DELETE FROM bucket_data
        WHERE group_id = ?
          AND bucket_name = ?
          AND op_id <= ?
        `,
        [this.groupId, bucket, lastOpId]
      );
      await this.executeRun(
        `
        INSERT INTO bucket_data (
          id,
          group_id,
          bucket_name,
          op_id,
          op,
          checksum,
          target_op
        ) VALUES (?, ?, ?, ?, 'CLEAR', ?, ?)
        `,
        [uuid.v4(), this.groupId, bucket, lastOpId, BigInt(checksum), targetOp]
      );
    }
  }

  private async executeAll<T>(sql: string, params: unknown[]): Promise<T[]> {
    return this.orm.em.getConnection().execute<T[]>(sql, params, 'all');
  }

  private async executeRun(sql: string, params: unknown[]): Promise<void> {
    await this.orm.em.getConnection().execute(sql, params, 'run');
  }
}

function compactBucketRowKey(row: CompactionBucketDataRow): string {
  return `${row.tableName}/${row.rowId}/${row.sourceTable}.${row.sourceKey == null ? '' : row.sourceKey.toString('base64')}`;
}

function rawBucketDataRow(row: RawBucketDataRow): CompactionBucketDataRow {
  return {
    id: row.id,
    bucketName: row.bucket_name,
    opId: BigInt(row.op_id),
    op: row.op,
    sourceTable: row.source_table,
    sourceKey: row.source_key == null ? null : Buffer.from(row.source_key),
    tableName: row.table_name,
    rowId: row.row_id,
    checksum: BigInt(row.checksum),
    targetOp: row.target_op == null ? null : BigInt(row.target_op)
  };
}

function rawParameterRow(row: RawParameterCompactionRow): ParameterCompactionRow {
  const sourceTable = row.source_table ?? row.sourceTable;
  const sourceKey = row.source_key ?? row.sourceKey;
  if (sourceTable == null || sourceKey == null) {
    throw new Error('Expected parameter compaction row source columns');
  }

  return {
    id: BigInt(row.id),
    sourceTable,
    sourceKey: Buffer.from(sourceKey),
    lookup: Buffer.from(row.lookup),
    bucketParameters: row.bucket_parameters ?? row.bucketParameters
  };
}

function parameterKey(row: ParameterCompactionRow): string {
  return `${row.lookup.toString('base64')}/${row.sourceTable}/${row.sourceKey.toString('base64')}`;
}

function normalizedBucketParameters(value: unknown): string {
  if (typeof value != 'string') {
    return JSON.stringify(value ?? null);
  }

  try {
    const parsed = JSON.parse(value);
    return typeof parsed == 'string' ? parsed : JSON.stringify(parsed);
  } catch {
    return value;
  }
}

function sameBucketParameters(left: unknown, right: unknown): boolean {
  return normalizedBucketParameters(left) == normalizedBucketParameters(right);
}

function isEmptyBucketParameters(value: unknown): boolean {
  return normalizedBucketParameters(value) == '[]';
}
