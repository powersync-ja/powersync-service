import * as lib_postgres from '@powersync/lib-service-postgres';
import { logger, ReplicationAssertionError } from '@powersync/lib-services-framework';
import { storage, utils } from '@powersync/service-core';
import * as pgwire from '@powersync/service-jpgwire';
import * as t from 'ts-codec';
import { BIGINT_MAX } from '../types/codecs.js';
import { models } from '../types/types.js';
import { sql } from '../utils/db.js';
import { pick } from '../utils/ts-codec.js';
import { encodedCacheKey } from './batch/OperationBatch.js';

interface CurrentBucketState {
  /** Bucket name */
  bucket: string;
  /**
   * Rows seen in the bucket, with the last op_id of each.
   */
  seen: Map<string, bigint>;
  /**
   * Estimated memory usage of the seen Map.
   */
  trackingSize: number;

  /**
   * Last (lowest) seen op_id that is not a PUT.
   */
  lastNotPut: bigint | null;

  /**
   * Number of REMOVE/MOVE operations seen since lastNotPut.
   */
  opsSincePut: number;
}

/**
 * Additional options, primarily for testing.
 */
export interface PostgresCompactOptions extends storage.CompactOptions {
  /** Minimum of 2 */
  clearBatchLimit?: number;
  /** Minimum of 1 */
  moveBatchLimit?: number;
  /** Minimum of 1 */
  moveBatchQueryLimit?: number;
}

const DEFAULT_CLEAR_BATCH_LIMIT = 5000;
const DEFAULT_MOVE_BATCH_LIMIT = 2000;
const DEFAULT_MOVE_BATCH_QUERY_LIMIT = 10_000;

/** This default is primarily for tests. */
const DEFAULT_MEMORY_LIMIT_MB = 64;

export class PostgresCompactor {
  private updates: pgwire.Statement[] = [];

  private idLimitBytes: number;
  private moveBatchLimit: number;
  private moveBatchQueryLimit: number;
  private clearBatchLimit: number;
  private maxOpId: bigint | undefined;
  private buckets: string[] | undefined;

  constructor(
    private db: lib_postgres.DatabaseClient,
    private group_id: number,
    options?: PostgresCompactOptions
  ) {
    this.idLimitBytes = (options?.memoryLimitMB ?? DEFAULT_MEMORY_LIMIT_MB) * 1024 * 1024;
    this.moveBatchLimit = options?.moveBatchLimit ?? DEFAULT_MOVE_BATCH_LIMIT;
    this.moveBatchQueryLimit = options?.moveBatchQueryLimit ?? DEFAULT_MOVE_BATCH_QUERY_LIMIT;
    this.clearBatchLimit = options?.clearBatchLimit ?? DEFAULT_CLEAR_BATCH_LIMIT;
    this.maxOpId = options?.maxOpId;
    this.buckets = options?.compactBuckets;
  }

  /**
   * Compact buckets by converting operations into MOVE and/or CLEAR operations.
   *
   * See /docs/compacting-operations.md for details.
   */
  async compact() {
    if (this.buckets) {
      for (let bucket of this.buckets) {
        // We can make this more efficient later on by iterating
        // through the buckets in a single query.
        // That makes batching more tricky, so we leave for later.
        await this.compactInternal(bucket);
      }
    } else {
      await this.compactInternal(undefined);
    }
  }

  async compactInternal(bucket: string | undefined) {
    const idLimitBytes = this.idLimitBytes;

    let currentState: CurrentBucketState | null = null;

    let bucketLower: string | null = null;
    let bucketUpper: string | null = null;

    if (bucket?.includes('[')) {
      // Exact bucket name
      bucketLower = bucket;
      bucketUpper = bucket;
    } else if (bucket) {
      // Bucket definition name
      bucketLower = `${bucket}[`;
      bucketUpper = `${bucket}[\uFFFF`;
    }

    let upperOpIdLimit = BIGINT_MAX;

    while (true) {
      const batch = await this.db.sql`
        SELECT
          op,
          op_id,
          source_table,
          table_name,
          row_id,
          source_key,
          bucket_name
        FROM
          bucket_data
        WHERE
          group_id = ${{ type: 'int4', value: this.group_id }}
          AND bucket_name LIKE COALESCE(${{ type: 'varchar', value: bucketLower }}, '%')
          AND op_id < ${{ type: 'int8', value: upperOpIdLimit }}
        ORDER BY
          bucket_name,
          op_id DESC
        LIMIT
          ${{ type: 'int4', value: this.moveBatchQueryLimit }}
      `
        .decoded(
          pick(models.BucketData, ['op', 'source_table', 'table_name', 'source_key', 'row_id', 'op_id', 'bucket_name'])
        )
        .rows();

      if (batch.length == 0) {
        // We've reached the end
        break;
      }

      // Set upperBound for the next batch
      upperOpIdLimit = batch[batch.length - 1].op_id;

      for (const doc of batch) {
        if (currentState == null || doc.bucket_name != currentState.bucket) {
          if (currentState != null && currentState.lastNotPut != null && currentState.opsSincePut >= 1) {
            // Important to flush before clearBucket()
            await this.flush();
            logger.info(
              `Inserting CLEAR at ${this.group_id}:${currentState.bucket}:${currentState.lastNotPut} to remove ${currentState.opsSincePut} operations`
            );

            const bucket = currentState.bucket;
            const clearOp = currentState.lastNotPut;
            // Free memory before clearing bucket
            currentState = null;
            await this.clearBucket(bucket, clearOp);
          }
          currentState = {
            bucket: doc.bucket_name,
            seen: new Map(),
            trackingSize: 0,
            lastNotPut: null,
            opsSincePut: 0
          };
        }

        if (this.maxOpId != null && doc.op_id > this.maxOpId) {
          continue;
        }

        let isPersistentPut = doc.op == 'PUT';

        if (doc.op == 'REMOVE' || doc.op == 'PUT') {
          const key = `${doc.table_name}/${doc.row_id}/${encodedCacheKey(doc.source_table!, doc.source_key!)}`;
          const targetOp = currentState.seen.get(utils.flatstr(key));
          if (targetOp) {
            // Will convert to MOVE, so don't count as PUT
            isPersistentPut = false;

            this.updates.push(sql`
              UPDATE bucket_data
              SET
                op = 'MOVE',
                target_op = ${{ type: 'int8', value: targetOp }},
                table_name = NULL,
                row_id = NULL,
                data = NULL,
                source_table = NULL,
                source_key = NULL
              WHERE
                group_id = ${{ type: 'int4', value: this.group_id }}
                AND bucket_name = ${{ type: 'varchar', value: doc.bucket_name }}
                AND op_id = ${{ type: 'int8', value: doc.op_id }}
            `);
          } else {
            if (currentState.trackingSize >= idLimitBytes) {
              // Reached memory limit.
              // Keep the highest seen values in this case.
            } else {
              // flatstr reduces the memory usage by flattening the string
              currentState.seen.set(utils.flatstr(key), doc.op_id);
              // length + 16 for the string
              // 24 for the bigint
              // 50 for map overhead
              // 50 for additional overhead
              currentState.trackingSize += key.length + 140;
            }
          }
        }

        if (isPersistentPut) {
          currentState.lastNotPut = null;
          currentState.opsSincePut = 0;
        } else if (doc.op != 'CLEAR') {
          if (currentState.lastNotPut == null) {
            currentState.lastNotPut = doc.op_id;
          }
          currentState.opsSincePut += 1;
        }

        if (this.updates.length >= this.moveBatchLimit) {
          await this.flush();
        }
      }
    }

    await this.flush();
    currentState?.seen.clear();
    if (currentState?.lastNotPut != null && currentState?.opsSincePut > 1) {
      logger.info(
        `Inserting CLEAR at ${this.group_id}:${currentState.bucket}:${currentState.lastNotPut} to remove ${currentState.opsSincePut} operations`
      );
      const bucket = currentState.bucket;
      const clearOp = currentState.lastNotPut;
      // Free memory before clearing bucket
      currentState = null;
      await this.clearBucket(bucket, clearOp);
    }
  }

  private async flush() {
    if (this.updates.length > 0) {
      logger.info(`Compacting ${this.updates.length} ops`);
      await this.db.query(...this.updates);
      this.updates = [];
    }
  }

  /**
   * Perform a CLEAR compact for a bucket.
   *
   * @param bucket bucket name
   * @param op op_id of the last non-PUT operation, which will be converted to CLEAR.
   */
  private async clearBucket(bucket: string, op: bigint) {
    /**
     * This entire method could be implemented as a Postgres function, but this might make debugging
     * a bit more challenging.
     */
    let done = false;
    while (!done) {
      await this.db.lockConnection(async (db) => {
        /**
         * Start a transaction where each read returns the state at the start of the transaction,.
         * Similar to the MongoDB readConcern: { level: 'snapshot' } mode.
         */
        await db.sql`BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ`.execute();

        try {
          let checksum = 0;
          let lastOpId: bigint | null = null;
          let targetOp: bigint | null = null;
          let gotAnOp = false;

          const codec = pick(models.BucketData, ['op', 'source_table', 'source_key', 'op_id', 'checksum', 'target_op']);
          for await (const operations of db.streamRows<t.Encoded<typeof codec>>(sql`
            SELECT
              source_table,
              source_key,
              op,
              op_id,
              checksum,
              target_op
            FROM
              bucket_data
            WHERE
              group_id = ${{ type: 'int4', value: this.group_id }}
              AND bucket_name = ${{ type: 'varchar', value: bucket }}
              AND op_id <= ${{ type: 'int8', value: op }}
            ORDER BY
              op_id
            LIMIT
              ${{ type: 'int4', value: this.clearBatchLimit }}
          `)) {
            const decodedOps = operations.map((o) => codec.decode(o));
            for (const op of decodedOps) {
              if ([models.OpType.MOVE, models.OpType.REMOVE, models.OpType.CLEAR].includes(op.op)) {
                checksum = utils.addChecksums(checksum, Number(op.checksum));
                lastOpId = op.op_id;
                if (op.op != models.OpType.CLEAR) {
                  gotAnOp = true;
                }
                if (op.target_op != null) {
                  if (targetOp == null || op.target_op > targetOp) {
                    targetOp = op.target_op;
                  }
                }
              } else {
                throw new ReplicationAssertionError(
                  `Unexpected ${op.op} operation at ${this.group_id}:${bucket}:${op.op_id}`
                );
              }
            }
          }

          if (!gotAnOp) {
            await db.sql`COMMIT`.execute();
            done = true;
            return;
          }

          logger.info(`Flushing CLEAR at ${lastOpId}`);

          await db.sql`
            DELETE FROM bucket_data
            WHERE
              group_id = ${{ type: 'int4', value: this.group_id }}
              AND bucket_name = ${{ type: 'varchar', value: bucket }}
              AND op_id <= ${{ type: 'int8', value: lastOpId }}
          `.execute();

          await db.sql`
            INSERT INTO
              bucket_data (
                group_id,
                bucket_name,
                op_id,
                op,
                checksum,
                target_op
              )
            VALUES
              (
                ${{ type: 'int4', value: this.group_id }},
                ${{ type: 'varchar', value: bucket }},
                ${{ type: 'int8', value: lastOpId }},
                ${{ type: 'varchar', value: models.OpType.CLEAR }},
                ${{ type: 'int8', value: checksum }},
                ${{ type: 'int8', value: targetOp }}
              )
          `.execute();

          await db.sql`COMMIT`.execute();
        } catch (ex) {
          await db.sql`ROLLBACK`.execute();
          throw ex;
        }
      });
    }
  }
}
