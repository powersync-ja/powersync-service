import * as lib_postgres from '@powersync/lib-service-postgres';
import { migrations } from '@powersync/lib-services-framework';
import { models } from '../types/types.js';
import { sql } from '../utils/db.js';

export type PostgresMigrationStoreOptions = {
  db: lib_postgres.DatabaseClient;
};

export class PostgresMigrationStore implements migrations.MigrationStore {
  constructor(protected options: PostgresMigrationStoreOptions) {}

  protected get db() {
    return this.options.db;
  }

  async init() {
    await this.db.query(sql`
      CREATE TABLE IF NOT EXISTS migrations (
        id SERIAL PRIMARY KEY,
        last_run TEXT,
        LOG JSONB NOT NULL
      );
    `);
  }

  async clear() {
    await this.db.query(sql`DELETE FROM migrations;`);
  }

  async load(): Promise<migrations.MigrationState | undefined> {
    const res = await this.db.sql`
      SELECT
        last_run,
        LOG
      FROM
        migrations
      LIMIT
        1
    `
      .decoded(models.Migration)
      .first();

    if (!res) {
      return;
    }

    return {
      last_run: res.last_run,
      log: res.log
    };
  }

  async save(state: migrations.MigrationState): Promise<void> {
    await this.db.query(sql`
      INSERT INTO
        migrations (id, last_run, LOG)
      VALUES
        (
          1,
          ${{ type: 'varchar', value: state.last_run }},
          ${{ type: 'jsonb', value: state.log }}
        )
      ON CONFLICT (id) DO UPDATE
      SET
        last_run = EXCLUDED.last_run,
        LOG = EXCLUDED.log;
    `);
  }
}
