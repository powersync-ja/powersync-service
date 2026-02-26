import * as lib_postgres from '@powersync/lib-service-postgres';
import { storage } from '@powersync/service-core';
import * as t from 'ts-codec';
import { pick } from '../utils/ts-codec.js';
import * as models from '../types/models/CurrentData.js';

type Queryable = Pick<lib_postgres.DatabaseClient, 'sql' | 'streamRows'>;

const TruncateCurrentDataCodec = pick(models.V1CurrentData, ['buckets', 'lookups', 'source_key']);
const LookupKeyCodec = pick(models.V1CurrentData, ['source_key', 'source_table']);

export type TruncateCurrentDataRow = t.Decoded<typeof TruncateCurrentDataCodec>;
export type CurrentDataLookupRow =
  | t.Decoded<typeof LookupKeyCodec>
  | t.Decoded<typeof models.V1CurrentData>
  | t.Decoded<typeof models.V3CurrentData>;

export const V1_CURRENT_DATA_TABLE = 'current_data';
export const V3_CURRENT_DATA_TABLE = 'v3_current_data';

export class PostgresCurrentDataStore {
  readonly table: string;
  readonly softDeleteEnabled: boolean;

  constructor(storageConfig: storage.StorageVersionConfig) {
    this.softDeleteEnabled = storageConfig.softDeleteCurrentData;
    this.table = storageConfig.softDeleteCurrentData ? V3_CURRENT_DATA_TABLE : V1_CURRENT_DATA_TABLE;
  }

  streamTruncateRows(
    db: Queryable,
    options: {
      groupId: number;
      sourceTableId: string;
      limit: number;
    }
  ) {
    return db.streamRows<t.Encoded<typeof TruncateCurrentDataCodec>>({
      statement: `
        SELECT
          buckets,
          lookups,
          source_key
        FROM
          ${this.table}
        WHERE
          group_id = $1
          AND source_table = $2
          ${this.wherePendingDelete({ onlyLiveRows: true })}
        LIMIT
          $3
        FOR NO KEY UPDATE
      `,
      params: [
        { type: 'int4', value: options.groupId },
        { type: 'varchar', value: options.sourceTableId },
        { type: 'int4', value: options.limit }
      ]
    });
  }

  decodeTruncateRow(row: t.Encoded<typeof TruncateCurrentDataCodec>): TruncateCurrentDataRow {
    return TruncateCurrentDataCodec.decode(row);
  }

  streamSizeRows(
    db: Queryable,
    options: {
      groupId: number;
      lookups: { source_table: string; source_key: string }[];
    }
  ) {
    return db.streamRows<{
      source_table: string;
      source_key: storage.ReplicaId;
      data_size: number;
    }>({
      statement: `
        WITH
          filter_data AS (
            SELECT
              decode(FILTER ->> 'source_key', 'hex') AS source_key,
              (FILTER ->> 'source_table') AS source_table_id
            FROM
              jsonb_array_elements($1::jsonb) AS FILTER
          )
        SELECT
          octet_length(c.data) AS data_size,
          c.source_table,
          c.source_key
        FROM
          ${this.table} c
          JOIN filter_data f ON c.source_table = f.source_table_id
          AND c.source_key = f.source_key
        WHERE
          c.group_id = $2
        FOR NO KEY UPDATE
      `,
      params: [
        { type: 'jsonb', value: options.lookups },
        { type: 'int4', value: options.groupId }
      ]
    });
  }

  streamLookupRows(
    db: Queryable,
    options: {
      groupId: number;
      lookups: { source_table: string; source_key: string }[];
      skipExistingRows: boolean;
    }
  ) {
    const selectColumns = options.skipExistingRows ? `c.source_table, c.source_key` : `c.*`;
    return db.streamRows<any>({
      statement: `
        SELECT
          ${selectColumns}
        FROM
          ${this.table} c
          JOIN (
            SELECT
              decode(FILTER ->> 'source_key', 'hex') AS source_key,
              FILTER ->> 'source_table' AS source_table_id
            FROM
              jsonb_array_elements($1::jsonb) AS FILTER
          ) f ON c.source_table = f.source_table_id
          AND c.source_key = f.source_key
        WHERE
          c.group_id = $2
        FOR NO KEY UPDATE;
      `,
      params: [
        { type: 'jsonb', value: options.lookups },
        { type: 'int4', value: options.groupId }
      ]
    });
  }

  decodeLookupRow(row: any, skipExistingRows: boolean): CurrentDataLookupRow {
    if (skipExistingRows) {
      return LookupKeyCodec.decode(row);
    }
    return this.softDeleteEnabled ? models.V3CurrentData.decode(row) : models.V1CurrentData.decode(row);
  }

  async flushUpserts(db: Queryable, updates: models.V3CurrentData[]) {
    if (updates.length == 0) {
      return;
    }

    if (this.softDeleteEnabled) {
      await db.sql`
        INSERT INTO
          v3_current_data (
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
          decode(source_key, 'hex') AS source_key,
          buckets::jsonb AS buckets,
          decode(data, 'hex') AS data,
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
            source_key text,
            buckets text,
            data text,
            lookups TEXT[],
            pending_delete bigint
          )
        ON CONFLICT (group_id, source_table, source_key) DO UPDATE
        SET
          buckets = EXCLUDED.buckets,
          data = EXCLUDED.data,
          lookups = EXCLUDED.lookups,
          pending_delete = EXCLUDED.pending_delete;
      `.execute();
      return;
    }

    await db.sql`
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
        decode(source_key, 'hex') AS source_key,
        buckets::jsonb AS buckets,
        decode(data, 'hex') AS data,
        array(
          SELECT
            decode(element, 'hex')
          FROM
            unnest(lookups) AS element
        ) AS lookups
      FROM
        json_to_recordset(${{ type: 'json', value: updates }}::json) AS t (
          group_id integer,
          source_table text,
          source_key text,
          buckets text,
          data text,
          lookups TEXT[]
        )
      ON CONFLICT (group_id, source_table, source_key) DO UPDATE
      SET
        buckets = EXCLUDED.buckets,
        data = EXCLUDED.data,
        lookups = EXCLUDED.lookups;
    `.execute();
  }

  async flushDeletes(
    db: Queryable,
    options: {
      groupId: number;
      deletes: { source_key_hex: string; source_table: string }[];
    }
  ) {
    if (options.deletes.length == 0) {
      return;
    }
    if (this.softDeleteEnabled) {
      await db.sql`
        WITH
          conditions AS (
            SELECT
              source_table,
              decode(source_key_hex, 'hex') AS source_key
            FROM
              jsonb_to_recordset(${{
          type: 'jsonb',
          value: options.deletes
        }}::jsonb) AS t (source_table text, source_key_hex text)
          )
        DELETE FROM v3_current_data USING conditions
        WHERE
          v3_current_data.group_id = ${{ type: 'int4', value: options.groupId }}
          AND v3_current_data.source_table = conditions.source_table
          AND v3_current_data.source_key = conditions.source_key;
      `.execute();
      return;
    }

    await db.sql`
      WITH
        conditions AS (
          SELECT
            source_table,
            decode(source_key_hex, 'hex') AS source_key
          FROM
            jsonb_to_recordset(${{
        type: 'jsonb',
        value: options.deletes
      }}::jsonb) AS t (source_table text, source_key_hex text)
        )
      DELETE FROM current_data USING conditions
      WHERE
        current_data.group_id = ${{ type: 'int4', value: options.groupId }}
        AND current_data.source_table = conditions.source_table
        AND current_data.source_key = conditions.source_key;
    `.execute();
  }

  async cleanupPendingDeletes(db: Queryable, options: { groupId: number; lastCheckpoint: bigint }) {
    if (!this.softDeleteEnabled) {
      return;
    }
    await db.sql`
      DELETE FROM v3_current_data
      WHERE
        group_id = ${{ type: 'int4', value: options.groupId }}
        AND pending_delete IS NOT NULL
        AND pending_delete <= ${{ type: 'int8', value: options.lastCheckpoint }}
    `.execute();
  }

  async deleteGroupRows(db: Queryable, options: { groupId: number }) {
    if (this.softDeleteEnabled) {
      await db.sql`
        DELETE FROM v3_current_data
        WHERE
          group_id = ${{ type: 'int4', value: options.groupId }}
      `.execute();
    } else {
      await db.sql`
        DELETE FROM current_data
        WHERE
          group_id = ${{ type: 'int4', value: options.groupId }}
      `.execute();
    }
  }

  private wherePendingDelete(options: { onlyLiveRows: boolean }) {
    if (this.softDeleteEnabled && options.onlyLiveRows) {
      return `AND pending_delete IS NULL`;
    }
    return ``;
  }
}
