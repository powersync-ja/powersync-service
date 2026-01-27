import * as lib_postgres from '@powersync/lib-service-postgres';
import { logger, ReplicationAssertionError } from '@powersync/lib-services-framework';
import { InternalOpId, storage, utils } from '@powersync/service-core';
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
  seen: Map<string, InternalOpId>;
  /**
   * Estimated memory usage of the seen Map.
   */
  trackingSize: number;

  /**
   * Last (lowest) seen op_id that is not a PUT.
   */
  lastNotPut: InternalOpId | null;

  /**
   * Number of REMOVE/MOVE operations seen since lastNotPut.
   */
  opsSincePut: number;
}

/**
 * Additional options, primarily for testing.
 */
export interface PostgresCompactOptions extends storage.CompactOptions {}

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
  private maxOpId: InternalOpId | undefined;
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
    // GJN: hack to simplify and remove the bucket-specific logic
    // if (this.buckets) {
    //   for (let bucket of this.buckets) {
    //     // We can make this more efficient later on by iterating
    //     // through the buckets in a single query.
    //     // That makes batching more tricky, so we leave for later.
    //     await this.compactInternal(bucket);
    //   }
    // } else {
    await this.compactInternal(undefined);
    // }
  }

  async compactInternal(bucket: string | undefined) {
    const idLimitBytes = this.idLimitBytes;

    let currentState: CurrentBucketState | null = null;

    // GJN: hack to simplify and remove the bucket-specific logic
    // result: compact will always compact all buckets (which is what we need for our use case anyway)
    // this lets us remove the lower bound logic from the query

    let bucketUpper: string | null = null;
    let upperOpIdLimit = BIGINT_MAX;

    // GJN: to avoid using collate, just select the max bucket name from the db to use as the initial upper bound
    // uses an index only scan, so this query is very fast (and only run once)
    // Limit  (cost=0.55..0.86 rows=1 width=60) (actual time=0.015..0.016 rows=1 loops=1)                                                                 
    //     -> Index Only Scan Backward using unique_id on bucket_data  (cost = 0.55..85662.92 rows = 275925 width = 60) (actual time = 0.014..0.015 rows = 1 loops = 1)
    //         Index Cond: (group_id = 25)                                                                                                                
    //         Heap Fetches: 0                                                                                                                            
    // Planning Time: 0.075 ms                                                                                                                            
    // Execution Time: 0.034 ms                                                                                                                           
    const maxBucketName = await this.db.sql`
      SELECT bucket_name
        FROM bucket_data
        WHERE group_id = ${{ type: 'int4', value: this.group_id }} 
        ORDER BY bucket_name DESC 
        LIMIT 1
    `
      .decoded(
        pick(models.BucketData, ['bucket_name'])
      )
      .rows();
    if (maxBucketName.length > 0) {
      bucketUpper = maxBucketName[0].bucket_name;
    }

    while (true) {
      // Query changes: 
      // - remove the lower bound logic from the query as it's not needed for our use case
      // - remove the collate logic from the query as it's not needed anymore since we selected the actual upper bound already

      // Previous execution plan: (sample query killed after 10 minutes!)
      // Limit(cost = 0.69..30274.90 rows = 10000 width = 195) |
      //   -> Index Scan Backward using unique_id on bucket_data  (cost = 0.69..42010218.77 rows = 13876569 width = 195)                                                                                                                               |
      //     Index Cond: ((group_id = 6) AND(bucket_name >= '':: text))                                                                                                                                                                       |
      //       Filter: (((bucket_name = 'common_team_data["f03cf665-aa30-48fa-b908-7cb81d36904f"]'::text) AND(op_id < '999999999999':: bigint)) OR(bucket_name < 'common_team_data["f03cf665-aa30-48fa-b908-7cb81d36904f"]':: text COLLATE "C"))|
      
      // Now using the new execution plan: (execution in about 15ms)
      // Limit(cost = 1240120.47..1241287.21 rows = 10000 width = 195)(actual time = 12.555..15.292 rows = 4226 loops = 1) |
      //   -> Gather Merge(cost = 1240120.47..1267395.77 rows = 233772 width = 195)(actual time = 12.554..15.003 rows = 4226 loops = 1) |
      //     Workers Planned: 2 |
      //       Workers Launched: 2 |
      //         -> Sort(cost = 1239120.44..1239412.66 rows = 116886 width = 195)(actual time = 5.327..5.417 rows = 1409 loops = 3) |
      //         Sort Key: bucket_name DESC, op_id DESC |
      //           Sort Method: quicksort  Memory: 915kB |
      //             Worker 0:  Sort Method: quicksort  Memory: 162kB |
      //               Worker 1:  Sort Method: quicksort  Memory: 159kB |
      //                 -> Parallel Bitmap Heap Scan on bucket_data(cost = 83107.46..1230770.27 rows = 116886 width = 195)(actual time = 0.465..1.476 rows = 1409 loops = 3) |
      //                   Recheck Cond: (((group_id = 6) AND(bucket_name = 'common_team_data["f03cf665-aa30-48fa-b908-7cb81d36904f"]':: text) AND(op_id < '999999999999':: bigint)) OR((group_id = 6) AND(bucket_name < 'common_team_data["f03cf665-aa30-48fa-b908-|
      //               Heap Blocks: exact = 1687 |
      //                   -> BitmapOr(cost = 83107.46..83107.46 rows = 280529 width = 0)(actual time = 0.703..0.704 rows = 0 loops = 1) |
      //                   -> Bitmap Index Scan on unique_id(cost = 0.00..67.33 rows = 211 width = 0)(actual time = 0.015..0.015 rows = 0 loops = 1) |
      //                   Index Cond: ((group_id = 6) AND(bucket_name = 'common_team_data["f03cf665-aa30-48fa-b908-7cb81d36904f"]':: text) AND(op_id < '999999999999':: bigint)) |
      //                     -> Bitmap Index Scan on unique_id(cost = 0.00..82899.87 rows = 280318 width = 0)(actual time = 0.687..0.687 rows = 4226 loops = 1) |
      //                   Index Cond: ((group_id = 6) AND(bucket_name < 'common_team_data["f03cf665-aa30-48fa-b908-7cb81d36904f"]':: text)) |
      //  Planning Time: 0.119 ms |
      //  Execution Time: 15.468 ms |
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
          AND (
            (
              bucket_name = ${{ type: 'varchar', value: bucketUpper }}
              AND op_id < ${{ type: 'int8', value: upperOpIdLimit }}
            )
            OR bucket_name < ${{ type: 'varchar', value: bucketUpper }}
          )
        ORDER BY
          bucket_name DESC,
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
      const lastBatchItem = batch[batch.length - 1];
      upperOpIdLimit = lastBatchItem.op_id;
      bucketUpper = lastBatchItem.bucket_name;

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
  private async clearBucket(bucket: string, op: InternalOpId) {
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
          let lastOpId: InternalOpId | null = null;
          let targetOp: InternalOpId | null = null;
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
