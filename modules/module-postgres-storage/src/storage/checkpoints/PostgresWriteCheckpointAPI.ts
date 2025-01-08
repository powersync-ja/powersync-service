import * as framework from '@powersync/lib-services-framework';
import { storage } from '@powersync/service-core';
import { JSONBig } from '@powersync/service-jsonbig';
import { models } from '../../types/types.js';
import { DatabaseClient } from '../../utils/connection/DatabaseClient.js';

export type PostgresCheckpointAPIOptions = {
  db: DatabaseClient;
  mode: storage.WriteCheckpointMode;
};

export class PostgresWriteCheckpointAPI implements storage.WriteCheckpointAPI {
  readonly db: DatabaseClient;
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

  async batchCreateCustomWriteCheckpoints(checkpoints: storage.CustomWriteCheckpointOptions[]): Promise<void> {
    return batchCreateCustomWriteCheckpoints(this.db, checkpoints);
  }

  async createCustomWriteCheckpoint(options: storage.CustomWriteCheckpointOptions): Promise<bigint> {
    if (this.writeCheckpointMode !== storage.WriteCheckpointMode.CUSTOM) {
      throw new framework.errors.ValidationError(
        `Creating a custom Write Checkpoint when the current Write Checkpoint mode is set to "${this.writeCheckpointMode}"`
      );
    }

    const { checkpoint, user_id, sync_rules_id } = options;
    const row = await this.db.sql`
      INSERT INTO
        custom_write_checkpoints (user_id, write_checkpoint, sync_rules_id)
      VALUES
        (
          ${{ type: 'varchar', value: user_id }},
          ${{ type: 'int8', value: checkpoint }},
          ${{ type: 'int4', value: sync_rules_id }}
        )
      ON CONFLICT DO
      UPDATE
      SET
        write_checkpoint = EXCLUDED.write_checkpoint
      RETURNING
        *;
    `
      .decoded(models.CustomWriteCheckpoint)
      .first();
    return row!.write_checkpoint;
  }

  async createManagedWriteCheckpoint(checkpoint: storage.ManagedWriteCheckpointOptions): Promise<bigint> {
    if (this.writeCheckpointMode !== storage.WriteCheckpointMode.MANAGED) {
      throw new framework.errors.ValidationError(
        `Attempting to create a managed Write Checkpoint when the current Write Checkpoint mode is set to "${this.writeCheckpointMode}"`
      );
    }

    const row = await this.db.sql`
      INSERT INTO
        write_checkpoints (user_id, lsns, write_checkpoint)
      VALUES
        (
          ${{ type: 'varchar', value: checkpoint.user_id }},
          ${{ type: 'jsonb', value: checkpoint.heads }},
          ${{ type: 'int8', value: 1 }}
        )
      ON CONFLICT (user_id) DO
      UPDATE
      SET
        write_checkpoint = write_checkpoints.write_checkpoint + 1,
        lsns = EXCLUDED.lsns
      RETURNING
        *;
    `
      .decoded(models.WriteCheckpoint)
      .first();
    return row!.write_checkpoint;
  }

  async lastWriteCheckpoint(filters: storage.LastWriteCheckpointFilters): Promise<bigint | null> {
    switch (this.writeCheckpointMode) {
      case storage.WriteCheckpointMode.CUSTOM:
        if (false == 'sync_rules_id' in filters) {
          throw new framework.errors.ValidationError(`Sync rules ID is required for custom Write Checkpoint filtering`);
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
  db: DatabaseClient,
  checkpoints: storage.CustomWriteCheckpointOptions[]
): Promise<void> {
  if (!checkpoints.length) {
    return;
  }

  await db.sql`
    WITH
      json_data AS (
        SELECT
          jsonb_array_elements(${{ type: 'jsonb', value: JSONBig.stringify(checkpoints) }}) AS
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
    ON CONFLICT (user_id, sync_rules_id) DO
    UPDATE
    SET
      write_checkpoint = EXCLUDED.write_checkpoint;
  `.execute();
}
