import { ServiceAssertionError } from '@powersync/lib-services-framework';
import * as pgwire from '@powersync/service-jpgwire';

export type MigrationFunction = (db: pgwire.PgConnection) => Promise<void>;

interface Migration {
  id: number;
  name: string;
  up: MigrationFunction;
}

// Very loosely based on https://github.com/porsager/postgres-shift/
export class Migrations {
  private migrations: Migration[] = [];

  add(id: number, name: string, up: MigrationFunction) {
    if (this.migrations.length > 0 && this.migrations[this.migrations.length - 1].id >= id) {
      throw new ServiceAssertionError('Migration ids must be strictly incrementing');
    }
    this.migrations.push({ id, up, name });
  }

  async up(db: pgwire.PgConnection) {
    await db.query('BEGIN');
    try {
      await this.ensureMigrationsTable(db);
      const current = await this.getCurrentMigration(db);
      let currentId = current ? current.id : 0;

      for (let migration of this.migrations) {
        if (migration.id <= currentId) {
          continue;
        }
        await migration.up(db);

        await db.query({
          statement: `
      insert into migrations (
        migration_id,
        name
      ) values (
        $1,
        $2
      )
    `,
          params: [
            { type: 'int4', value: migration.id },
            { type: 'varchar', value: migration.name }
          ]
        });
      }

      await db.query('COMMIT');
    } catch (e) {
      await db.query('ROLLBACK');
      throw e;
    }
  }

  getCurrentMigration(db: pgwire.PgConnection) {
    return db
      .query(
        `
      select migration_id as id from migrations
      order by migration_id desc
      limit 1
    `
      )
      .then((results) => ({ id: results.rows[0][0] as number }));
  }

  async ensureMigrationsTable(db: pgwire.PgConnection) {
    await db.query(`create table if not exists migrations (
        migration_id serial primary key,
        created_at timestamp with time zone not null default now(),
        name text
      )
    `);
  }
}
