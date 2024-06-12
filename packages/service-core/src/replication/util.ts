import * as pgwire from '@powersync/service-jpgwire';

import * as util from '../util/util-index.js';
import { ReplicationColumn, ReplicationIdentity } from './PgRelation.js';

export interface ReplicaIdentityResult {
  columns: ReplicationColumn[];
  replicationIdentity: ReplicationIdentity;
}

export async function getPrimaryKeyColumns(
  db: pgwire.PgClient,
  relationId: number,
  mode: 'primary' | 'replident'
): Promise<ReplicationColumn[]> {
  const indexFlag = mode == 'primary' ? `i.indisprimary` : `i.indisreplident`;
  const attrRows = await util.retriedQuery(db, {
    statement: `SELECT a.attname as name, a.atttypid as typeid, a.attnum as attnum
                                    FROM pg_index i
                                             JOIN pg_attribute a
                                                  ON a.attrelid = i.indrelid
                                                      AND a.attnum = ANY (i.indkey)
                                    WHERE i.indrelid = $1::oid
                                      AND ${indexFlag}
                                      AND a.attnum > 0
                                      ORDER BY a.attnum`,
    params: [{ value: relationId, type: 'int4' }]
  });

  return attrRows.rows.map((row) => {
    return { name: row[0] as string, typeOid: row[1] as number };
  });
}

export async function getAllColumns(db: pgwire.PgClient, relationId: number): Promise<ReplicationColumn[]> {
  const attrRows = await util.retriedQuery(db, {
    statement: `SELECT a.attname as name, a.atttypid as typeid, a.attnum as attnum
                                    FROM pg_attribute a
                                    WHERE a.attrelid = $1::oid
                                      AND attnum > 0
                                    ORDER BY a.attnum`,
    params: [{ type: 'varchar', value: relationId }]
  });
  return attrRows.rows.map((row) => {
    return { name: row[0] as string, typeOid: row[1] as number };
  });
}

export async function getReplicationIdentityColumns(
  db: pgwire.PgClient,
  relationId: number
): Promise<ReplicaIdentityResult> {
  const rows = await util.retriedQuery(db, {
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
    return { replicationIdentity: 'nothing', columns: [] };
  } else if (idType == 'full') {
    return { replicationIdentity: 'full', columns: await getAllColumns(db, relationId) };
  } else if (idType == 'default') {
    return { replicationIdentity: 'default', columns: await getPrimaryKeyColumns(db, relationId, 'primary') };
  } else if (idType == 'index') {
    return { replicationIdentity: 'index', columns: await getPrimaryKeyColumns(db, relationId, 'replident') };
  } else {
    return { replicationIdentity: 'nothing', columns: [] };
  }
}
