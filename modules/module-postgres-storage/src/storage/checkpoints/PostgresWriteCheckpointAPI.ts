import * as lib_postgres from '@powersync/lib-service-postgres';
import * as framework from '@powersync/lib-services-framework';
import { InternalOpId, storage } from '@powersync/service-core';
import { models } from '../../types/types.js';

export type PostgresCheckpointAPIOptions = {
  db: lib_postgres.DatabaseClient;
  mode: storage.WriteCheckpointMode;
};

export class PostgresWriteCheckpointAPI implements storage.WriteCheckpointAPI {
  readonly db: lib_postgres.DatabaseClient;
  private _mode: storage.WriteCheckpointMode;

  constructor(options: PostgresCheckpointAPIOptions) {
    this.db = options.db;
    this._mode = options.mode;
  }

  get writeCheckpointMode() {
    return this._mode;
  }

  setWriteCheckpointMode(mode: storage.WriteCheckpointMode): void {
    this._mode = mode;
  }

  async batchCreateCustomWriteCheckpoints(
    checkpoints: storage.CustomWriteCheckpointOptions[],
    op_id: InternalOpId
  ): Promise<void> {
    return batchCreateCustomWriteCheckpoints(this.db, checkpoints);
  }

  async createManagedWriteCheckpoints(
    checkpoints: storage.ManagedWriteCheckpointOptions[]
  ): Promise<Map<string, bigint>> {
    if (this.writeCheckpointMode !== storage.WriteCheckpointMode.MANAGED) {
      throw new framework.errors.ValidationError(
        `Attempting to create a managed Write Checkpoint when the current Write Checkpoint mode is set to "${this.writeCheckpointMode}"`
      );
    }

    const uniqueCheckpoints = [...new Map(checkpoints.map((checkpoint) => [checkpoint.user_id, checkpoint])).values()];
    if (uniqueCheckpoints.length == 0) {
      return new Map();
    }

    const rows: models.WriteCheckpointDecoded[] = [];
    const generatedCheckpoints = uniqueCheckpoints.filter((checkpoint) => checkpoint.checkpoint_request_id == null);
    const suppliedCheckpoints = uniqueCheckpoints.filter((checkpoint) => checkpoint.checkpoint_request_id != null);

    if (generatedCheckpoints.length > 0) {
      const mappedCheckpoints = generatedCheckpoints.map((checkpoint) => ({
        user_id: checkpoint.user_id,
        lsns: checkpoint.heads
      }));

      const generatedRows = await this.db.sql`
        WITH
          json_data AS (
            SELECT
            CHECKPOINT ->> 'user_id' AS user_id,
            CHECKPOINT -> 'lsns' AS lsns
            FROM
              jsonb_array_elements(${{ type: 'jsonb', value: mappedCheckpoints }}) AS
            CHECKPOINT
          )
        INSERT INTO
          write_checkpoints (user_id, lsns, write_checkpoint)
        SELECT
          user_id,
          lsns,
          1
        FROM
          json_data
        ON CONFLICT (user_id) DO UPDATE
        SET
          write_checkpoint = write_checkpoints.write_checkpoint + 1,
          lsns = EXCLUDED.lsns
        RETURNING
          *;
      `
        .decoded(models.WriteCheckpoint)
        .rows();

      rows.push(...generatedRows);
    }

    if (suppliedCheckpoints.length > 0) {
      // Supplied request ids are monotonic: only a value greater than the stored
      // write_checkpoint may update the checkpoint id and heads. Stale or
      // duplicate requests return the stored id.
      const mappedCheckpoints = suppliedCheckpoints.map((checkpoint) => ({
        user_id: checkpoint.user_id,
        lsns: checkpoint.heads,
        checkpoint_request_id: String(checkpoint.checkpoint_request_id)
      }));

      const suppliedRows = await this.db.sql`
        WITH
          json_data AS (
            SELECT
            CHECKPOINT ->> 'user_id' AS user_id,
            CHECKPOINT -> 'lsns' AS lsns,
            (
              CHECKPOINT ->> 'checkpoint_request_id'
            )::int8 AS checkpoint_request_id
            FROM
              jsonb_array_elements(${{ type: 'jsonb', value: mappedCheckpoints }}) AS
            CHECKPOINT
          )
        INSERT INTO
          write_checkpoints (user_id, lsns, write_checkpoint)
        SELECT
          user_id,
          lsns,
          checkpoint_request_id
        FROM
          json_data
        ON CONFLICT (user_id) DO UPDATE
        SET
          write_checkpoint = CASE
            WHEN EXCLUDED.write_checkpoint > write_checkpoints.write_checkpoint THEN EXCLUDED.write_checkpoint
            ELSE write_checkpoints.write_checkpoint
          END,
          lsns = CASE
            WHEN EXCLUDED.write_checkpoint > write_checkpoints.write_checkpoint THEN EXCLUDED.lsns
            ELSE write_checkpoints.lsns
          END
        RETURNING
          *;
      `
        .decoded(models.WriteCheckpoint)
        .rows();

      rows.push(...suppliedRows);
    }

    return new Map(rows.map((row) => [row.user_id, row.write_checkpoint]));
  }

  async lastWriteCheckpoint(filters: storage.LastWriteCheckpointFilters): Promise<bigint | null> {
    switch (this.writeCheckpointMode) {
      case storage.WriteCheckpointMode.CUSTOM:
        if (false == 'sync_rules_id' in filters) {
          throw new framework.errors.ValidationError(
            `Replication stream ID is required for custom Write Checkpoint filtering`
          );
        }
        return this.lastCustomWriteCheckpoint(filters as storage.CustomWriteCheckpointFilters);
      case storage.WriteCheckpointMode.MANAGED:
        if (false == 'heads' in filters) {
          throw new framework.errors.ValidationError(
            `Replication HEAD is required for managed Write Checkpoint filtering`
          );
        }
        return this.lastManagedWriteCheckpoint(filters as storage.ManagedWriteCheckpointFilters);
    }
  }

  protected async lastCustomWriteCheckpoint(filters: storage.CustomWriteCheckpointFilters) {
    const { user_id, sync_rules_id } = filters;
    const row = await this.db.sql`
      SELECT
        *
      FROM
        custom_write_checkpoints
      WHERE
        user_id = ${{ type: 'varchar', value: user_id }}
        AND sync_rules_id = ${{ type: 'int4', value: sync_rules_id }}
    `
      .decoded(models.CustomWriteCheckpoint)
      .first();
    return row?.write_checkpoint ?? null;
  }

  protected async lastManagedWriteCheckpoint(filters: storage.ManagedWriteCheckpointFilters) {
    const { user_id, heads } = filters;
    // TODO: support multiple heads when we need to support multiple connections
    const lsn = heads['1'];
    if (lsn == null) {
      // Can happen if we haven't replicated anything yet.
      return null;
    }
    const row = await this.db.sql`
      SELECT
        *
      FROM
        write_checkpoints
      WHERE
        user_id = ${{ type: 'varchar', value: user_id }}
        AND lsns ->> '1' <= ${{ type: 'varchar', value: lsn }};
    `
      .decoded(models.WriteCheckpoint)
      .first();
    return row?.write_checkpoint ?? null;
  }
}

export async function batchCreateCustomWriteCheckpoints(
  db: lib_postgres.DatabaseClient,
  checkpoints: storage.CustomWriteCheckpointOptions[]
): Promise<void> {
  if (!checkpoints.length) {
    return;
  }

  // Needs to be encoded using plain JSON.stringify
  const mappedCheckpoints = checkpoints.map((cp) => {
    return {
      user_id: cp.user_id,
      // Cannot encode bigint directly using JSON.stringify.
      // The ::int8 in the query below will take care of casting back to a number
      checkpoint: String(cp.checkpoint),
      sync_rules_id: cp.sync_rules_id
    };
  });

  await db.sql`
    WITH
      json_data AS (
        SELECT
          jsonb_array_elements(${{ type: 'jsonb', value: mappedCheckpoints }}) AS
        CHECKPOINT
      )
    INSERT INTO
      custom_write_checkpoints (user_id, write_checkpoint, sync_rules_id)
    SELECT
    CHECKPOINT ->> 'user_id'::varchar,
    (
      CHECKPOINT ->> 'checkpoint'
    )::int8,
    (
      CHECKPOINT ->> 'sync_rules_id'
    )::int4
    FROM
      json_data
    ON CONFLICT (user_id, sync_rules_id) DO UPDATE
    SET
      write_checkpoint = EXCLUDED.write_checkpoint;
  `.execute();
}
