import * as lib_postgres from '@powersync/lib-service-postgres';
import { logger } from '@powersync/lib-services-framework';
import { bson, storage, utils } from '@powersync/service-core';
import { JSONBig } from '@powersync/service-jsonbig';
import * as sync_rules from '@powersync/service-sync-rules';
import { models, RequiredOperationBatchLimits } from '../../types/types.js';
import { replicaIdToSubkey } from '../../utils/bson.js';
import { PostgresCurrentDataStore } from '../current-data-store.js';
import { postgresTableId } from '../table-id.js';

export type SaveBucketDataOptions = {
  /**
   * This value will be serialized into a BSON Byte array for storage
   */
  source_key: storage.ReplicaId;
  table: storage.SourceTable;
  before_buckets: models.CurrentBucket[];
  evaluated: sync_rules.EvaluatedRow[];
};

export type SaveParameterDataOptions = {
  source_key: storage.ReplicaId;
  table: storage.SourceTable;
  evaluated: sync_rules.EvaluatedParameters[];
  existing_lookups: Buffer[];
};

export type DeleteCurrentDataOptions = {
  source_table_id: string;
  /**
   * ReplicaID which needs to be serialized in order to be queried
   * or inserted into the DB
   */
  source_key?: storage.ReplicaId;
  /**
   * Optionally provide the serialized source key directly
   */
  serialized_source_key?: Buffer;

  /**
   * Streaming replication needs soft deletes, while truncating tables can use a hard delete directly.
   */
  soft: boolean;
};

export type PostgresPersistedBatchOptions = RequiredOperationBatchLimits & {
  group_id: number;
  storageConfig: storage.StorageVersionConfig;
  /**
   * The replication transaction the batch is written on.
   */
  db: lib_postgres.WrappedConnection;
};

const EMPTY_DATA = Buffer.from(bson.serialize({}));

/**
 * Accumulates the operations of one replication transaction and writes them to Postgres.
 *
 * bucket_data and bucket_parameters rows are written while they accumulate: once the pending rows
 * of a table would exceed `maxStatementSize`, they are inserted on the transaction before the next
 * row is added. This bounds both the memory held for a single source row evaluated into many
 * buckets and the size of the JSON parameter of each statement. current_data is written in `flush()`.
 *
 * Since `saveBucketData()` and `saveParameterData()` may write on the connection, no streamed query
 * may be in progress on it while they are called: the connection processes responses in order, and
 * a streamed query only completes once its rows are consumed.
 *
 * The transaction limits in `shouldFlushTransaction()` count everything written on the transaction,
 * including the rows already inserted.
 */
export class PostgresPersistedBatch {
  group_id: number;
  private readonly db: lib_postgres.WrappedConnection;
  private readonly storageConfig: storage.StorageVersionConfig;
  private readonly currentDataStore: PostgresCurrentDataStore;

  readonly maxTransactionBatchSize: number;
  readonly maxTransactionDocCount: number;
  /**
   * Pending bucket_data or bucket_parameters rows are inserted before their estimated size exceeds this.
   */
  readonly maxStatementSize: number;

  /**
   * Ordered set of bucket_data insert operation parameters, not inserted yet.
   */
  protected bucketDataInserts: models.BucketData[];
  protected parameterDataInserts: models.BucketParameters[];
  /**
   * This is stored as a map to avoid multiple inserts (or conflicts) for the same key.
   *
   * Each key may only occur in one of these two maps.
   */
  protected currentDataInserts: Map<string, models.V3CurrentData>;
  protected currentDataDeletes: Map<string, { source_key_hex: string; source_table: string }>;

  /**
   * Very rough estimates of the size in bytes of the operations not inserted yet.
   */
  private pendingBucketDataSize = 0;
  private pendingParameterDataSize = 0;
  private pendingCurrentDataSize = 0;

  /**
   * Rows and estimated size already inserted on the transaction before `flush()`.
   */
  private insertedBucketDataCount = 0;
  private insertedParameterDataCount = 0;
  private insertedSize = 0;
  /**
   * Statements issued while accumulating, excluding the ones issued by `flush()`.
   */
  private earlyStatementCount = 0;

  constructor(options: PostgresPersistedBatchOptions) {
    this.group_id = options.group_id;
    this.db = options.db;
    this.storageConfig = options.storageConfig;
    this.currentDataStore = new PostgresCurrentDataStore(this.storageConfig);

    this.maxTransactionBatchSize = options.max_estimated_size;
    this.maxTransactionDocCount = options.max_record_count;
    this.maxStatementSize = Math.min(options.max_estimated_size, options.max_statement_size);

    this.bucketDataInserts = [];
    this.parameterDataInserts = [];
    this.currentDataInserts = new Map();
    this.currentDataDeletes = new Map();
  }

  /**
   * Very rough estimate of the size in bytes of all operations in this transaction,
   * including the rows already inserted.
   */
  get currentSize() {
    return this.insertedSize + this.pendingBucketDataSize + this.pendingParameterDataSize + this.pendingCurrentDataSize;
  }

  async saveBucketData(options: SaveBucketDataOptions) {
    const remaining_buckets = new Map<string, models.CurrentBucket>();
    for (const b of options.before_buckets) {
      const key = currentBucketKey(b);
      remaining_buckets.set(key, b);
    }

    const dchecksum = utils.hashDelete(replicaIdToSubkey(options.table.id, options.source_key));

    const serializedSourceKey = storage.serializeReplicaId(options.source_key);
    const hexSourceKey = serializedSourceKey.toString('hex');

    for (const k of options.evaluated) {
      const key = currentBucketKey(k);
      remaining_buckets.delete(key);

      const data = JSONBig.stringify(k.data);
      const checksum = utils.hashData(k.table, k.id, data);

      await this.addBucketData(
        {
          group_id: this.group_id,
          bucket_name: k.bucket,
          op: models.OpType.PUT,
          source_table: postgresTableId(options.table.id),
          source_key: hexSourceKey,
          table_name: k.table,
          row_id: k.id,
          checksum,
          data,
          op_id: 0, // Will use nextval of sequence
          target_op: null
        },
        k.bucket.length + data.length + hexSourceKey.length + 100
      );
    }

    for (const bd of remaining_buckets.values()) {
      // REMOVE operation
      await this.addBucketData(
        {
          group_id: this.group_id,
          bucket_name: bd.bucket,
          op: models.OpType.REMOVE,
          source_table: postgresTableId(options.table.id),
          source_key: hexSourceKey,
          table_name: bd.table,
          row_id: bd.id,
          checksum: dchecksum,
          op_id: 0, // Will use nextval of sequence
          target_op: null,
          data: null
        },
        bd.bucket.length + hexSourceKey.length + 100
      );
    }
  }

  /**
   * Inserts the pending bucket_data rows first if adding this row would exceed the statement size.
   */
  private async addBucketData(row: models.BucketData, estimatedSize: number) {
    if (this.bucketDataInserts.length > 0 && this.pendingBucketDataSize + estimatedSize > this.maxStatementSize) {
      await this.flushBucketData();
      this.earlyStatementCount += 1;
    }
    this.bucketDataInserts.push(row);
    this.pendingBucketDataSize += estimatedSize;
  }

  async saveParameterData(options: SaveParameterDataOptions) {
    // This is similar to saving bucket data.
    // A key difference is that we don't need to keep the history intact.
    // We do need to keep track of recent history though - enough that we can get consistent data for any specific checkpoint.
    // Instead of storing per bucket id, we store per "lookup".
    // A key difference is that we don't need to store or keep track of anything per-bucket - the entire record is
    // either persisted or removed.
    // We also don't need to keep history intact.
    const { source_key, table, evaluated, existing_lookups } = options;
    const serializedSourceKey = storage.serializeReplicaId(source_key);
    const hexSourceKey = serializedSourceKey.toString('hex');
    const remaining_lookups = new Map<string, Buffer>();
    for (const l of existing_lookups) {
      remaining_lookups.set(l.toString('base64'), l);
    }

    // 1. Insert new entries
    for (const result of evaluated) {
      const binLookup = storage.serializeLookupBuffer(result.lookup);
      const base64 = binLookup.toString('base64');
      remaining_lookups.delete(base64);
      const hexLookup = binLookup.toString('hex');
      const serializedBucketParameters = JSONBig.stringify(result.bucketParameters);
      await this.addParameterData(
        {
          group_id: this.group_id,
          source_table: postgresTableId(table.id),
          source_key: hexSourceKey,
          bucket_parameters: serializedBucketParameters,
          id: 0, // auto incrementing id
          lookup: hexLookup
        },
        hexLookup.length + serializedBucketParameters.length + hexSourceKey.length + 100
      );
    }

    // 2. "REMOVE" entries for any lookup not touched.
    for (const lookup of remaining_lookups.values()) {
      const hexLookup = lookup.toString('hex');
      await this.addParameterData(
        {
          group_id: this.group_id,
          source_table: postgresTableId(table.id),
          source_key: hexSourceKey,
          bucket_parameters: JSON.stringify([]),
          id: 0, // auto incrementing id
          lookup: hexLookup
        },
        hexLookup.length + hexSourceKey.length + 100
      );
    }
  }

  /**
   * Inserts the pending bucket_parameters rows first if adding this row would exceed the statement size.
   */
  private async addParameterData(row: models.BucketParameters, estimatedSize: number) {
    if (this.parameterDataInserts.length > 0 && this.pendingParameterDataSize + estimatedSize > this.maxStatementSize) {
      await this.flushParameterData();
      this.earlyStatementCount += 1;
    }
    this.parameterDataInserts.push(row);
    this.pendingParameterDataSize += estimatedSize;
  }

  deleteCurrentData(options: DeleteCurrentDataOptions) {
    if (options.soft && this.storageConfig.softDeleteCurrentData) {
      return this.upsertCurrentData(
        {
          group_id: this.group_id,
          source_table: options.source_table_id,
          source_key: options.source_key,
          buckets: [],
          data: EMPTY_DATA,
          lookups: [],
          pending_delete: 1n // converted to nextval('op_id_sequence') in the query
        },
        options.serialized_source_key
      );
    } else {
      const serializedReplicaId = options.serialized_source_key ?? storage.serializeReplicaId(options.source_key);
      const hexReplicaId = serializedReplicaId.toString('hex');
      const source_table = options.source_table_id;
      const key = `${this.group_id}-${source_table}-${hexReplicaId}`;
      this.currentDataInserts.delete(key);
      this.currentDataDeletes.set(key, {
        source_key_hex: hexReplicaId,
        source_table: source_table
      });
      this.pendingCurrentDataSize += serializedReplicaId.byteLength + 100;
    }
  }

  upsertCurrentData(options: models.V3CurrentDataDecoded, serialized_source_key?: Buffer) {
    const { source_table, source_key, buckets } = options;

    const serializedReplicaId = serialized_source_key ?? storage.serializeReplicaId(source_key);
    const hexReplicaId = serializedReplicaId.toString('hex');
    const serializedBuckets = JSONBig.stringify(options.buckets);

    /**
     * Only track the last unique ID for this current_data record.
     * Applying multiple items in the flush method could cause an
     * "
     *   ON CONFLICT DO UPDATE command cannot affect row a second time
     * "
     * error.
     */
    const key = `${this.group_id}-${source_table}-${hexReplicaId}`;

    this.currentDataDeletes.delete(key);
    this.currentDataInserts.set(key, {
      group_id: this.group_id,
      source_table: source_table,
      source_key: hexReplicaId,
      buckets: serializedBuckets,
      data: options.data.toString('hex'),
      lookups: options.lookups.map((l) => l.toString('hex')),
      pending_delete: options.pending_delete?.toString() ?? null
    });

    this.pendingCurrentDataSize +=
      (options.data?.byteLength ?? 0) +
      serializedReplicaId.byteLength +
      buckets.length +
      options.lookups.reduce((total, l) => {
        return total + l.byteLength;
      }, 0) +
      100;
  }

  shouldFlushTransaction() {
    return (
      this.currentSize >= this.maxTransactionBatchSize ||
      this.insertedBucketDataCount + this.bucketDataInserts.length >= this.maxTransactionDocCount ||
      this.currentDataInserts.size >= this.maxTransactionDocCount ||
      this.currentDataDeletes.size >= this.maxTransactionDocCount ||
      this.insertedParameterDataCount + this.parameterDataInserts.length >= this.maxTransactionDocCount
    );
  }

  async flush() {
    const stats = {
      bucketDataCount: this.insertedBucketDataCount + this.bucketDataInserts.length,
      parameterDataCount: this.insertedParameterDataCount + this.parameterDataInserts.length,
      currentDataCount: this.currentDataInserts.size + this.currentDataDeletes.size
    };
    const flushedAny = stats.bucketDataCount > 0 || stats.parameterDataCount > 0 || stats.currentDataCount > 0;

    logger.info(
      `powersync_${this.group_id} Flushed ${stats.bucketDataCount} + ${stats.parameterDataCount} + ${
        this.currentDataInserts.size
      } updates, ${Math.round(this.currentSize / 1024)}kb.`
    );
    if (this.earlyStatementCount > 0) {
      logger.debug(
        `powersync_${this.group_id} Inserted ${this.insertedBucketDataCount} + ${this.insertedParameterDataCount} rows in ${this.earlyStatementCount} statements while accumulating.`
      );
    }

    // Flush current_data before the remaining rows, since this is where lock errors are most likely
    // to occur, and we want to detect those as soon as possible. The rows inserted while accumulating
    // are already on the transaction at this point, so a lock error on a transaction with early
    // inserts also rolls back that work. This only affects transactions with rows of large fan-out.
    await this.flushCurrentData();

    await this.flushBucketData();
    await this.flushParameterData();
    this.currentDataInserts = new Map();
    this.currentDataDeletes = new Map();
    this.pendingCurrentDataSize = 0;
    this.insertedBucketDataCount = 0;
    this.insertedParameterDataCount = 0;
    this.insertedSize = 0;
    this.earlyStatementCount = 0;

    return {
      ...stats,
      flushedAny
    };
  }

  /**
   * Inserts the pending bucket_data rows in a single statement.
   *
   * Sequential statements in the same transaction preserve the order of the operations,
   * since op_id is assigned from the sequence per row.
   */
  protected async flushBucketData() {
    const rows = this.bucketDataInserts;
    if (rows.length == 0) {
      return;
    }

    await this.db.sql`
      INSERT INTO
        bucket_data (
          group_id,
          bucket_name,
          op_id,
          op,
          source_table,
          source_key,
          table_name,
          row_id,
          checksum,
          data,
          target_op
        )
      SELECT
        group_id,
        bucket_name,
        nextval('op_id_sequence'),
        op,
        source_table,
        decode(source_key, 'hex') AS source_key,
        table_name,
        row_id,
        checksum,
        data,
        target_op
      FROM
        json_to_recordset(${{ type: 'json', value: rows }}::json) AS t (
          group_id integer,
          bucket_name text,
          source_table text,
          source_key text, -- Input as hex string
          table_name text,
          op text,
          row_id text,
          checksum bigint,
          data text,
          target_op bigint
        );
    `.execute();

    this.insertedBucketDataCount += rows.length;
    this.insertedSize += this.pendingBucketDataSize;
    this.bucketDataInserts = [];
    this.pendingBucketDataSize = 0;
  }

  /**
   * Inserts the pending bucket_parameters rows in a single statement.
   */
  protected async flushParameterData() {
    const rows = this.parameterDataInserts;
    if (rows.length == 0) {
      return;
    }

    await this.db.sql`
      INSERT INTO
        bucket_parameters (
          group_id,
          source_table,
          source_key,
          lookup,
          bucket_parameters
        )
      SELECT
        group_id,
        source_table,
        decode(source_key, 'hex') AS source_key, -- Decode hex to bytea
        decode(lookup, 'hex') AS lookup, -- Decode hex to bytea
        bucket_parameters
      FROM
        json_to_recordset(${{ type: 'json', value: rows }}::json) AS t (
          group_id integer,
          source_table text,
          source_key text, -- Input as hex string
          lookup text, -- Input as hex string
          bucket_parameters text -- Input as stringified JSON
        )
    `.execute();

    this.insertedParameterDataCount += rows.length;
    this.insertedSize += this.pendingParameterDataSize;
    this.parameterDataInserts = [];
    this.pendingParameterDataSize = 0;
  }

  protected async flushCurrentData() {
    if (this.currentDataInserts.size > 0) {
      const updates = Array.from(this.currentDataInserts.values());
      // Sort by source_table, source_key to ensure consistent order.
      // While order of updates don't directly matter, using a consistent order helps to reduce 40P01 deadlock errors.
      // We may still have deadlocks between deletes and inserts, but those should be less frequent.
      updates.sort((a, b) => {
        if (a.source_table < b.source_table) return -1;
        if (a.source_table > b.source_table) return 1;
        if (a.source_key < b.source_key) return -1;
        if (a.source_key > b.source_key) return 1;
        return 0;
      });

      await this.currentDataStore.flushUpserts(this.db, updates);
    }

    if (this.currentDataDeletes.size > 0) {
      const deletes = Array.from(this.currentDataDeletes.values());
      // Same sorting as for inserts
      deletes.sort((a, b) => {
        if (a.source_table < b.source_table) return -1;
        if (a.source_table > b.source_table) return 1;
        if (a.source_key_hex < b.source_key_hex) return -1;
        if (a.source_key_hex > b.source_key_hex) return 1;
        return 0;
      });

      await this.currentDataStore.flushDeletes(this.db, {
        groupId: this.group_id,
        deletes
      });
    }
  }
}

export function currentBucketKey(b: models.CurrentBucket) {
  return `${b.bucket}/${b.table}/${b.id}`;
}
