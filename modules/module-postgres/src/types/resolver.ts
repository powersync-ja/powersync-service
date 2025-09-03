import { DatabaseInputRow, SqliteInputRow, toSyncRulesRow } from '@powersync/service-sync-rules';
import * as pgwire from '@powersync/service-jpgwire';
import { CustomTypeRegistry } from './registry.js';
import semver from 'semver';
import { getServerVersion } from '../utils/postgres_version.js';

/**
 * Resolves descriptions used to decode values for custom postgres types.
 *
 * Custom types are resolved from the source database, which also involves crawling inner types (e.g. for composites).
 */
export class PostgresTypeResolver {
  private cachedVersion: semver.SemVer | null = null;

  constructor(
    readonly registry: CustomTypeRegistry,
    private readonly pool: pgwire.PgClient
  ) {
    this.registry = new CustomTypeRegistry();
  }

  private async fetchVersion(): Promise<semver.SemVer> {
    if (this.cachedVersion == null) {
      this.cachedVersion = (await getServerVersion(this.pool)) ?? semver.parse('0.0.1');
    }

    return this.cachedVersion!;
  }

  /**
   * @returns Whether the Postgres instance this type cache is connected to has support for the multirange type (which
   * is the case for Postgres 14 and later).
   */
  async supportsMultiRanges() {
    const version = await this.fetchVersion();
    return version.compare(PostgresTypeResolver.minVersionForMultirange) >= 0;
  }

  /**
   * Fetches information about indicated types.
   *
   * If a type references another custom type (e.g. because it's a composite type with a custom field), these are
   * automatically crawled as well.
   */
  public async fetchTypes(oids: number[]) {
    const multiRangeSupport = await this.supportsMultiRanges();

    let pending = oids.filter((id) => !this.registry.knows(id));
    // For details on columns, see https://www.postgresql.org/docs/current/catalog-pg-type.html
    const multiRangeDesc = `WHEN 'm' THEN json_build_object('inner', (SELECT rngsubtype FROM pg_range WHERE rngmultitypid = t.oid))`;
    const statement = `
SELECT oid, t.typtype,
    CASE t.typtype
        WHEN 'b' THEN json_build_object('element_type', t.typelem, 'delim', (SELECT typdelim FROM pg_type i WHERE i.oid = t.typelem))
        WHEN 'd' THEN json_build_object('type', t.typbasetype)
        WHEN 'c' THEN json_build_object(
            'elements',
            (SELECT json_agg(json_build_object('name', a.attname, 'type', a.atttypid))
                FROM pg_attribute a
                WHERE a.attrelid = t.typrelid)
        )
        WHEN 'r' THEN json_build_object('inner', (SELECT rngsubtype FROM pg_range WHERE rngtypid = t.oid))
        ${multiRangeSupport ? multiRangeDesc : ''}
        ELSE NULL
    END AS desc
FROM pg_type t
WHERE t.oid = ANY($1)
`;

    while (pending.length != 0) {
      // 1016: int8 array
      const query = await this.pool.query({ statement, params: [{ type: 1016, value: pending }] });
      const stillPending: number[] = [];

      const requireType = (oid: number) => {
        if (!this.registry.knows(oid) && !pending.includes(oid) && !stillPending.includes(oid)) {
          stillPending.push(oid);
        }
      };

      for (const row of pgwire.pgwireRows(query)) {
        const oid = Number(row.oid);
        const desc = JSON.parse(row.desc);

        switch (row.typtype) {
          case 'b':
            const { element_type, delim } = desc;

            if (!this.registry.knows(oid)) {
              // This type is an array of another custom type.
              const inner = Number(element_type);
              if (inner != 0) {
                // Some array types like macaddr[] don't seem to have their inner type set properly - skip!
                requireType(inner);
                this.registry.set(oid, {
                  type: 'array',
                  innerId: inner,
                  separatorCharCode: (delim as string).charCodeAt(0),
                  sqliteType: () => 'text' // Since it's JSON
                });
              }
            }
            break;
          case 'c':
            // For composite types, we sync the JSON representation.
            const elements: { name: string; typeId: number }[] = [];
            for (const { name, type } of desc.elements) {
              const typeId = Number(type);
              elements.push({ name, typeId });
              requireType(typeId);
            }

            this.registry.set(oid, {
              type: 'composite',
              members: elements,
              sqliteType: () => 'text' // Since it's JSON
            });
            break;
          case 'd':
            // For domain values like CREATE DOMAIN api.rating_value AS FLOAT CHECK (VALUE BETWEEN 0 AND 5), we sync
            // the inner type (pg_type.typbasetype).
            const inner = Number(desc.type);
            this.registry.setDomainType(oid, inner);
            requireType(inner);
            break;
          case 'r':
          case 'm': {
            const inner = Number(desc.inner);
            this.registry.set(oid, {
              type: row.typtype == 'r' ? 'range' : 'multirange',
              innerId: inner,
              sqliteType: () => 'text' // Since it's JSON
            });
          }
        }
      }

      pending = stillPending;
    }
  }

  /**
   * Crawls all custom types referenced by table columns in the current database.
   */
  public async fetchTypesForSchema() {
    const sql = `
SELECT DISTINCT a.atttypid AS type_oid
FROM pg_attribute a
JOIN pg_class c ON c.oid = a.attrelid
JOIN pg_namespace cn ON cn.oid = c.relnamespace
JOIN pg_type t ON t.oid = a.atttypid
JOIN pg_namespace tn ON tn.oid = t.typnamespace
WHERE a.attnum > 0
  AND NOT a.attisdropped
  AND cn.nspname not in ('information_schema', 'pg_catalog', 'pg_toast')
    `;

    const query = await this.pool.query({ statement: sql });
    let ids: number[] = [];
    for (const row of pgwire.pgwireRows(query)) {
      ids.push(Number(row.type_oid));
    }

    await this.fetchTypes(ids);
  }

  /**
   * pgwire message -> SQLite row.
   * @param message
   */
  constructAfterRecord(message: pgwire.PgoutputInsert | pgwire.PgoutputUpdate): SqliteInputRow {
    const rawData = (message as any).afterRaw;

    const record = this.decodeTuple(message.relation, rawData);
    return toSyncRulesRow(record);
  }

  /**
   * pgwire message -> SQLite row.
   * @param message
   */
  constructBeforeRecord(message: pgwire.PgoutputDelete | pgwire.PgoutputUpdate): SqliteInputRow | undefined {
    const rawData = (message as any).beforeRaw;
    if (rawData == null) {
      return undefined;
    }
    const record = this.decodeTuple(message.relation, rawData);
    return toSyncRulesRow(record);
  }

  /**
   * We need a high level of control over how values are decoded, to make sure there is no loss
   * of precision in the process.
   */
  decodeTuple(relation: pgwire.PgoutputRelation, tupleRaw: Record<string, any>): DatabaseInputRow {
    let result: Record<string, any> = {};
    for (let columnName in tupleRaw) {
      const rawval = tupleRaw[columnName];
      const typeOid = (relation as any)._tupleDecoder._typeOids.get(columnName);
      if (typeof rawval == 'string' && typeOid) {
        result[columnName] = this.registry.decodeDatabaseValue(rawval, typeOid);
      } else {
        result[columnName] = rawval;
      }
    }
    return result;
  }

  private static minVersionForMultirange: semver.SemVer = semver.parse('14.0.0')!;
}
