import * as lib_postgres from '@powersync/lib-service-postgres';
import { migrations } from '@powersync/lib-services-framework';
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
    const res = await this.db.queryRows<{ last_run: string; log: string }>(sql`
      SELECT
        last_run,
        LOG
      FROM
        migrations
      LIMIT
        1
    `);

    if (res.length === 0) {
      return;
    }

    const { last_run, log } = res[0];

    return {
      last_run: last_run,
      log: log ? JSON.parse(log) : []
    };
  }

  async save(state: migrations.MigrationState): Promise<void> {
    await this.db.query(sql`
      INSERT INTO
        migrations (last_run, LOG)
      VALUES
        (
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
