import { logger } from '@powersync/lib-services-framework';
import { storage, utils } from '@powersync/service-core';
import { JSONBig } from '@powersync/service-jsonbig';
import * as sync_rules from '@powersync/service-sync-rules';
import { models, RequiredOperationBatchLimits } from '../../types/types.js';
import { replicaIdToSubkey } from '../../utils/bson.js';
import { WrappedConnection } from '../../utils/connection/WrappedConnection.js';

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
  source_table_id: bigint;
  source_key: storage.ReplicaId;
};

export type PostgresPersistedBatchOptions = RequiredOperationBatchLimits & {
  group_id: number;
};

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
  protected currentDataDeletes: Pick<models.CurrentData, 'group_id' | 'source_key' | 'source_table'>[];
  /**
   * This is stored as a map to avoid multiple inserts (or conflicts) for the same key
   */
  protected currentDataInserts: Map<string, models.CurrentData>;

  constructor(options: PostgresPersistedBatchOptions) {
    this.group_id = options.group_id;

    this.maxTransactionBatchSize = options.max_estimated_size;
    this.maxTransactionDocCount = options.max_record_count;

    this.bucketDataInserts = [];
    this.parameterDataInserts = [];
    this.currentDataDeletes = [];
    this.currentDataInserts = new Map();
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
        source_table: options.table.id,
        source_key: hexSourceKey,
        table_name: k.table,
        row_id: k.id,
        checksum,
        data,
        op_id: 0, // Will use nextval of sequence
        target_op: null
      });

      this.currentSize += k.bucket.length * 2 + data.length * 2 + hexSourceKey.length * 2 + 100;
    }

    for (const bd of remaining_buckets.values()) {
      // REMOVE operation
      this.bucketDataInserts.push({
        group_id: this.group_id,
        bucket_name: bd.bucket,
        op: models.OpType.REMOVE,
        source_table: options.table.id,
        source_key: hexSourceKey,
        table_name: bd.table,
        row_id: bd.id,
        checksum: dchecksum,
        op_id: 0, // Will use nextval of sequence
        target_op: null,
        data: null
      });
      this.currentSize += bd.bucket.length * 2 + hexSourceKey.length * 2 + 100;
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
      const serializedBucketParameters = JSONBig.stringify(result.bucket_parameters);
      this.parameterDataInserts.push({
        group_id: this.group_id,
        source_table: table.id,
        source_key: hexSourceKey,
        bucket_parameters: serializedBucketParameters,
        id: 0, // auto incrementing id
        lookup: hexLookup
      });
      this.currentSize += hexLookup.length * 2 + serializedBucketParameters.length * 2 + hexSourceKey.length * 2 + 100;
    }

    // 2. "REMOVE" entries for any lookup not touched.
    for (const lookup of remaining_lookups.values()) {
      const hexLookup = lookup.toString('hex');
      this.parameterDataInserts.push({
        group_id: this.group_id,
        source_table: table.id,
        source_key: hexSourceKey,
        bucket_parameters: JSON.stringify([]),
        id: 0, // auto incrementing id
        lookup: hexLookup
      });
      this.currentSize += hexLookup.length * 2 + hexSourceKey.length * 2 + 100;
    }
  }

  deleteCurrentData(options: DeleteCurrentDataOptions) {
    const serializedReplicaId = storage.serializeReplicaId(options.source_key);
    this.currentDataDeletes.push({
      group_id: this.group_id,
      source_table: options.source_table_id.toString(),
      source_key: serializedReplicaId.toString('hex')
    });
    this.currentSize += serializedReplicaId.byteLength * 2 + 100;
  }

  upsertCurrentData(options: models.CurrentDataDecoded) {
    const { source_table, source_key, buckets } = options;

    const serializedReplicaId = storage.serializeReplicaId(source_key);
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

    this.currentDataInserts.set(key, {
      group_id: this.group_id,
      source_table: source_table,
      source_key: hexReplicaId,
      buckets: serializedBuckets,
      data: options.data.toString('hex'),
      lookups: options.lookups.map((l) => l.toString('hex'))
    });

    this.currentSize +=
      (options.data?.byteLength ?? 0) +
      serializedReplicaId.byteLength +
      buckets.length * 2 +
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
      this.currentDataDeletes.length >= this.maxTransactionDocCount ||
      this.parameterDataInserts.length >= this.maxTransactionDocCount
    );
  }

  async flush(db: WrappedConnection) {
    logger.info(
      `powersync_${this.group_id} Flushed ${this.bucketDataInserts.length} + ${this.parameterDataInserts.length} + ${
        this.currentDataInserts.size + this.currentDataDeletes.length
      } updates, ${Math.round(this.currentSize / 1024)}kb.`
    );

    await this.flushBucketData(db);
    await this.flushParameterData(db);
    await this.flushCurrentData(db);

    this.bucketDataInserts = [];
    this.parameterDataInserts = [];
    this.currentDataDeletes = [];
    this.currentDataInserts = new Map();
    this.currentSize = 0;
  }

  protected async flushBucketData(db: WrappedConnection) {
    if (this.bucketDataInserts.length > 0) {
      await db.sql`
        WITH
          parsed_data AS (
            SELECT
              group_id,
              bucket_name,
              source_table,
              decode(source_key, 'hex') AS source_key, -- Decode hex to bytea
              table_name,
              op,
              row_id,
              checksum,
              data,
              target_op
            FROM
              jsonb_to_recordset(${{ type: 'jsonb', value: this.bucketDataInserts }}::jsonb) AS t (
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
              )
          )
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
          source_key, -- Already decoded
          table_name,
          row_id,
          checksum,
          data,
          target_op
        FROM
          parsed_data;
      `.execute();
    }
  }

  protected async flushParameterData(db: WrappedConnection) {
    if (this.parameterDataInserts.length > 0) {
      await db.sql`
        WITH
          parsed_data AS (
            SELECT
              group_id,
              source_table,
              decode(source_key, 'hex') AS source_key, -- Decode hex to bytea
              decode(lookup, 'hex') AS lookup, -- Decode hex to bytea
              bucket_parameters::jsonb AS bucket_parameters
            FROM
              jsonb_to_recordset(${{ type: 'jsonb', value: this.parameterDataInserts }}::jsonb) AS t (
                group_id integer,
                source_table text,
                source_key text, -- Input as hex string
                lookup text, -- Input as hex string
                bucket_parameters text -- Input as stringified jsonb
              )
          )
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
          source_key, -- Already decoded
          lookup, -- Already decoded
          bucket_parameters
        FROM
          parsed_data;
      `.execute();
    }
  }

  protected async flushCurrentData(db: WrappedConnection) {
    if (this.currentDataInserts.size > 0) {
      await db.sql`
        WITH
          parsed_data AS (
            SELECT
              group_id,
              source_table,
              decode(source_key, 'hex') AS source_key, -- Decode hex to bytea
              buckets::jsonb AS buckets,
              decode(data, 'hex') AS data, -- Decode hex to bytea
              ARRAY(
                SELECT
                  decode((value ->> 0)::TEXT, 'hex')
                FROM
                  jsonb_array_elements(lookups::jsonb) AS value
              ) AS lookups -- Decode array of hex strings to bytea[]
            FROM
              jsonb_to_recordset(${{
          type: 'jsonb',
          value: Array.from(this.currentDataInserts.values())
        }}::jsonb) AS t (
                group_id integer,
                source_table text,
                source_key text, -- Input as hex string
                buckets text,
                data text, -- Input as hex string
                lookups text -- Input as stringified JSONB array of hex strings
              )
          )
        INSERT INTO
          current_data (
            group_id,
            source_table,
            source_key,
            buckets,
            data,
            lookups
          )
        SELECT
          group_id,
          source_table,
          source_key, -- Already decoded
          buckets,
          data, -- Already decoded
          lookups -- Already decoded
        FROM
          parsed_data
        ON CONFLICT (group_id, source_table, source_key) DO UPDATE
        SET
          buckets = EXCLUDED.buckets,
          data = EXCLUDED.data,
          lookups = EXCLUDED.lookups;
      `.execute();
    }

    if (this.currentDataDeletes.length > 0) {
      await db.sql`
        WITH
          conditions AS (
            SELECT
              group_id,
              source_table,
              decode(source_key, 'hex') AS source_key -- Decode hex to bytea
            FROM
              jsonb_to_recordset(${{ type: 'jsonb', value: this.currentDataDeletes }}::jsonb) AS t (
                group_id integer,
                source_table text,
                source_key text -- Input as hex string
              )
          )
        DELETE FROM current_data USING conditions
        WHERE
          current_data.group_id = conditions.group_id
          AND current_data.source_table = conditions.source_table
          AND current_data.source_key = conditions.source_key;
      `.execute();
    }
  }
}

export function currentBucketKey(b: models.CurrentBucket) {
  return `${b.bucket}/${b.table}/${b.id}`;
}
