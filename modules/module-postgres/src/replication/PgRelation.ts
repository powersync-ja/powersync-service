import { PgoutputRelation } from '@powersync/service-jpgwire';

export interface PgRelation {
  readonly relationId: number;
  readonly schema: string;
  readonly name: string;
  readonly replicaIdentity: ReplicationIdentity;
  readonly replicationColumns: ReplicationColumn[];
}

export type ReplicationIdentity = 'default' | 'nothing' | 'full' | 'index';

export interface ReplicationColumn {
  readonly name: string;
  readonly typeOid: number;
}

export function getReplicaIdColumns(relation: PgoutputRelation): ReplicationColumn[] {
  if (relation.replicaIdentity == 'nothing') {
    return [];
  } else {
    return relation.columns.filter((c) => (c.flags & 0b1) != 0).map((c) => ({ name: c.name, typeOid: c.typeOid }));
  }
}
export function getRelId(source: PgoutputRelation): number {
  // Source types are wrong here
  const relId = (source as any).relationOid as number;
  if (relId == null || typeof relId != 'number') {
    throw new Error(`No relation id!`);
  }
  return relId;
}

export function getPgOutputRelation(source: PgoutputRelation): PgRelation {
  return {
    name: source.name,
    schema: source.schema,
    relationId: getRelId(source),
    replicaIdentity: source.replicaIdentity,
    replicationColumns: getReplicaIdColumns(source)
  };
}
