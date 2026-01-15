import * as lib_postgres from '@powersync/lib-service-postgres';
import { logger, ServiceAssertionError } from '@powersync/lib-services-framework';
import { bson, InternalOpId, storage, utils } from '@powersync/service-core';
import { JSONBig } from '@powersync/service-jsonbig';
import * as sync_rules from '@powersync/service-sync-rules';
import { models, RequiredOperationBatchLimits } from '../../types/types.js';
import { replicaIdToSubkey } from '../../utils/bson.js';

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
};

const EMPTY_DATA = Buffer.from(bson.serialize({}));

export class PostgresPersistedBatch {
  group_id: number;

  /**
   * Very rough estimate of current operations size in bytes
   */
  currentSize: number;

  readonly maxTransactionBatchSize: number;
  readonly maxTransactionDocCount: number;

  /**
   * Ordered set of bucket_data insert operation parameters
   */
  protected bucketDataInserts: models.BucketData[];
  protected parameterDataInserts: models.BucketParameters[];
  /**
   * This is stored as a map to avoid multiple inserts (or conflicts) for the same key.
   *
   * Each key may only occur in one of these two maps.
   */
  protected currentDataInserts: Map<string, models.CurrentData>;
  protected currentDataDeletes: Map<string, { source_key_hex: string; source_table: string }>;

  constructor(options: PostgresPersistedBatchOptions) {
    this.group_id = options.group_id;

    this.maxTransactionBatchSize = options.max_estimated_size;
    this.maxTransactionDocCount = options.max_record_count;

    this.bucketDataInserts = [];
    this.parameterDataInserts = [];
    this.currentDataInserts = new Map();
    this.currentDataDeletes = new Map();
    this.currentSize = 0;
  }

  saveBucketData(options: SaveBucketDataOptions) {
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

      this.bucketDataInserts.push({
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
      });

      this.currentSize += k.bucket.length + data.length + hexSourceKey.length + 100;
    }

    for (const bd of remaining_buckets.values()) {
      // REMOVE operation
      this.bucketDataInserts.push({
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
      });
      this.currentSize += bd.bucket.length + hexSourceKey.length + 100;
    }
  }

  saveParameterData(options: SaveParameterDataOptions) {
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
      this.parameterDataInserts.push({
        group_id: this.group_id,
        source_table: postgresTableId(table.id),
        source_key: hexSourceKey,
        bucket_parameters: serializedBucketParameters,
        id: 0, // auto incrementing id
        lookup: hexLookup
      });
      this.currentSize += hexLookup.length + serializedBucketParameters.length + hexSourceKey.length + 100;
    }

    // 2. "REMOVE" entries for any lookup not touched.
    for (const lookup of remaining_lookups.values()) {
      const hexLookup = lookup.toString('hex');
      this.parameterDataInserts.push({
        group_id: this.group_id,
        source_table: postgresTableId(table.id),
        source_key: hexSourceKey,
        bucket_parameters: JSON.stringify([]),
        id: 0, // auto incrementing id
        lookup: hexLookup
      });
      this.currentSize += hexLookup.length + hexSourceKey.length + 100;
    }
  }

  deleteCurrentData(options: DeleteCurrentDataOptions) {
    if (options.soft) {
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
    }
  }

  upsertCurrentData(options: models.CurrentDataDecoded, serialized_source_key?: Buffer) {
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

    this.currentSize +=
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
      this.bucketDataInserts.length >= this.maxTransactionDocCount ||
      this.currentDataInserts.size >= this.maxTransactionDocCount ||
      this.parameterDataInserts.length >= this.maxTransactionDocCount
    );
  }

  async flush(db: lib_postgres.WrappedConnection) {
    const stats = {
      bucketDataCount: this.bucketDataInserts.length,
      parameterDataCount: this.parameterDataInserts.length,
      currentDataCount: this.currentDataInserts.size + this.currentDataDeletes.size
    };
    const flushedAny = stats.bucketDataCount > 0 || stats.parameterDataCount > 0 || stats.currentDataCount > 0;

    logger.info(
      `powersync_${this.group_id} Flushed ${this.bucketDataInserts.length} + ${this.parameterDataInserts.length} + ${
        this.currentDataInserts.size
      } updates, ${Math.round(this.currentSize / 1024)}kb.`
    );

    // Flush current_data first, since this is where lock errors are most likely to occur, and we
    // want to detect those as soon as possible.
    await this.flushCurrentData(db);

    await this.flushBucketData(db);
    await this.flushParameterData(db);
    this.bucketDataInserts = [];
    this.parameterDataInserts = [];
    this.currentDataInserts = new Map();
    this.currentDataDeletes = new Map();
    this.currentSize = 0;

    return {
      ...stats,
      flushedAny
    };
  }

  protected async flushBucketData(db: lib_postgres.WrappedConnection) {
    if (this.bucketDataInserts.length > 0) {
      await db.sql`
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
          json_to_recordset(${{ type: 'json', value: this.bucketDataInserts }}::json) AS t (
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
    }
  }

  protected async flushParameterData(db: lib_postgres.WrappedConnection) {
    if (this.parameterDataInserts.length > 0) {
      await db.sql`
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
          json_to_recordset(${{ type: 'json', value: this.parameterDataInserts }}::json) AS t (
            group_id integer,
            source_table text,
            source_key text, -- Input as hex string
            lookup text, -- Input as hex string
            bucket_parameters text -- Input as stringified JSON
          )
      `.execute();
    }
  }

  protected async flushCurrentData(db: lib_postgres.WrappedConnection) {
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

      await db.sql`
        INSERT INTO
          current_data (
            group_id,
            source_table,
            source_key,
            buckets,
            data,
            lookups,
            pending_delete
          )
        SELECT
          group_id,
          source_table,
          decode(source_key, 'hex') AS source_key, -- Decode hex to bytea
          buckets::jsonb AS buckets,
          decode(data, 'hex') AS data, -- Decode hex to bytea
          array(
            SELECT
              decode(element, 'hex')
            FROM
              unnest(lookups) AS element
          ) AS lookups,
          CASE
            WHEN pending_delete IS NOT NULL THEN nextval('op_id_sequence')
            ELSE NULL
          END AS pending_delete
        FROM
          json_to_recordset(${{ type: 'json', value: updates }}::json) AS t (
            group_id integer,
            source_table text,
            source_key text, -- Input as hex string
            buckets text,
            data text, -- Input as hex string
            lookups TEXT[], -- Input as stringified JSONB array of hex strings
            pending_delete bigint
          )
        ON CONFLICT (group_id, source_table, source_key) DO UPDATE
        SET
          buckets = EXCLUDED.buckets,
          data = EXCLUDED.data,
          lookups = EXCLUDED.lookups,
          pending_delete = EXCLUDED.pending_delete;
      `.execute();
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

      await db.sql`
        WITH
          conditions AS (
            SELECT
              source_table,
              decode(source_key_hex, 'hex') AS source_key -- Decode hex to bytea
            FROM
              jsonb_to_recordset(${{
          type: 'jsonb',
          value: deletes
        }}::jsonb) AS t (source_table text, source_key_hex text)
          )
        DELETE FROM current_data USING conditions
        WHERE
          current_data.group_id = ${{ type: 'int4', value: this.group_id }}
          AND current_data.source_table = conditions.source_table
          AND current_data.source_key = conditions.source_key;
      `.execute();
    }
  }
}

export function currentBucketKey(b: models.CurrentBucket) {
  return `${b.bucket}/${b.table}/${b.id}`;
}

export function postgresTableId(id: storage.SourceTableId) {
  if (typeof id == 'string') {
    return id;
  }
  throw new ServiceAssertionError(`Expected string table id, got ObjectId`);
}
