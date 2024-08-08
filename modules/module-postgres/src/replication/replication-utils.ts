import * as pgwire from '@powersync/service-jpgwire';

import { storage } from '@powersync/service-core';
import * as pgwire_utils from '../utils/pgwire_utils.js';
import { ReplicationIdentity } from './PgRelation.js';

export interface ReplicaIdentityResult {
  replicationColumns: storage.ColumnDescriptor[];
  replicationIdentity: ReplicationIdentity;
}

export async function getPrimaryKeyColumns(
  db: pgwire.PgClient,
  relationId: number,
  mode: 'primary' | 'replident'
): Promise<storage.ColumnDescriptor[]> {
  const indexFlag = mode == 'primary' ? `i.indisprimary` : `i.indisreplident`;
  const attrRows = await pgwire_utils.retriedQuery(db, {
    statement: `SELECT a.attname as name, a.atttypid as typeid, t.typname as type, a.attnum as attnum
                                    FROM pg_index i
                                    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey)
                                    JOIN pg_type t ON a.atttypid = t.oid
                                    WHERE i.indrelid = $1::oid
                                      AND ${indexFlag}
                                      AND a.attnum > 0
                                      ORDER BY a.attnum`,
    params: [{ value: relationId, type: 'int4' }]
  });

  return attrRows.rows.map((row) => {
    return { name: row[0] as string, typeOid: row[1] as number, type: row[2] as string };
  });
}

export async function getAllColumns(db: pgwire.PgClient, relationId: number): Promise<storage.ColumnDescriptor[]> {
  const attrRows = await pgwire_utils.retriedQuery(db, {
    statement: `SELECT a.attname as name, a.atttypid as typeid, t.typname as type, a.attnum as attnum
                                    FROM pg_attribute a
                                    JOIN pg_type t ON a.atttypid = t.oid
                                    WHERE a.attrelid = $1::oid
                                      AND attnum > 0
                                    ORDER BY a.attnum`,
    params: [{ type: 'varchar', value: relationId }]
  });
  return attrRows.rows.map((row) => {
    return { name: row[0] as string, typeOid: row[1] as number, type: row[2] as string };
  });
}

export async function getReplicationIdentityColumns(
  db: pgwire.PgClient,
  relationId: number
): Promise<ReplicaIdentityResult> {
  const rows = await pgwire_utils.retriedQuery(db, {
    statement: `SELECT CASE relreplident
        WHEN 'd' THEN 'default'
        WHEN 'n' THEN 'nothing'
        WHEN 'f' THEN 'full'
        WHEN 'i' THEN 'index'
     END AS replica_identity
FROM pg_class
WHERE oid = $1::oid LIMIT 1`,
    params: [{ type: 'int8', value: relationId }]
  });
  const idType: string = rows.rows[0]?.[0];
  if (idType == 'nothing' || idType == null) {
    return { replicationIdentity: 'nothing', replicationColumns: [] };
  } else if (idType == 'full') {
    return { replicationIdentity: 'full', replicationColumns: await getAllColumns(db, relationId) };
  } else if (idType == 'default') {
    return {
      replicationIdentity: 'default',
      replicationColumns: await getPrimaryKeyColumns(db, relationId, 'primary')
    };
  } else if (idType == 'index') {
    return {
      replicationIdentity: 'index',
      replicationColumns: await getPrimaryKeyColumns(db, relationId, 'replident')
    };
  } else {
    return { replicationIdentity: 'nothing', replicationColumns: [] };
  }
}

export async function checkSourceConfiguration(db: pgwire.PgClient) {
  // TODO: configurable
  const publication_name = 'powersync';

  // Check basic config
  await pgwire_utils.retriedQuery(
    db,
    `DO $$
BEGIN
if current_setting('wal_level') is distinct from 'logical' then
raise exception 'wal_level must be set to ''logical'', your database has it set to ''%''. Please edit your config file and restart PostgreSQL.', current_setting('wal_level');
end if;
if (current_setting('max_replication_slots')::int >= 1) is not true then
raise exception 'Your max_replication_slots setting is too low, it must be greater than 1. Please edit your config file and restart PostgreSQL.';
end if;
if (current_setting('max_wal_senders')::int >= 1) is not true then
raise exception 'Your max_wal_senders setting is too low, it must be greater than 1. Please edit your config file and restart PostgreSQL.';
end if;
end;
$$ LANGUAGE plpgsql;`
  );

  // Check that publication exists
  const rs = await pgwire_utils.retriedQuery(db, {
    statement: `SELECT * FROM pg_publication WHERE pubname = $1`,
    params: [{ type: 'varchar', value: publication_name }]
  });
  const row = pgwire.pgwireRows(rs)[0];
  if (row == null) {
    throw new Error(
      `Publication '${publication_name}' does not exist. Run: \`CREATE PUBLICATION ${publication_name} FOR ALL TABLES\`, or read the documentation for details.`
    );
  }
  if (row.pubinsert == false || row.pubupdate == false || row.pubdelete == false || row.pubtruncate == false) {
    throw new Error(
      `Publication '${publication_name}' does not publish all changes. Create a publication using \`WITH (publish = "insert, update, delete, truncate")\` (the default).`
    );
  }
  if (row.pubviaroot) {
    throw new Error(`'${publication_name}' uses publish_via_partition_root, which is not supported.`);
  }
}
