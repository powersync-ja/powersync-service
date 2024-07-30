import { PgoutputRelation } from '@powersync/service-jpgwire';
import { ColumnDescriptor, SourceEntityDescriptor } from '../storage/SourceEntity.js';

export type ReplicationIdentity = 'default' | 'nothing' | 'full' | 'index';

export function getReplicaIdColumns(relation: PgoutputRelation): ColumnDescriptor[] {
  if (relation.replicaIdentity == 'nothing') {
    return [];
  } else {
    return relation.columns
      .filter((c) => (c.flags & 0b1) != 0)
      .map((c) => ({ name: c.name, typeId: c.typeOid, type: c.typeName ?? '' }));
  }
}
export function getRelId(source: PgoutputRelation): number {
  // Source types are wrong here
  const relId = (source as any).relationOid as number;
  if (!relId) {
    throw new Error(`No relation id!`);
  }
  return relId;
}

export function getPgOutputRelation(source: PgoutputRelation): SourceEntityDescriptor {
  return {
    name: source.name,
    schema: source.schema,
    objectId: getRelId(source),
    replicationColumns: getReplicaIdColumns(source)
  };
}
