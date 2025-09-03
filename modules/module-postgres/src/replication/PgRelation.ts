import { ReplicationAssertionError } from '@powersync/lib-services-framework';
import { storage } from '@powersync/service-core';
import { PgoutputRelation } from '@powersync/service-jpgwire';

export type ReplicationIdentity = 'default' | 'nothing' | 'full' | 'index';

export function getReplicaIdColumns(relation: PgoutputRelation): storage.ColumnDescriptor[] {
  if (relation.replicaIdentity == 'nothing') {
    return [];
  } else {
    return relation.columns
      .filter((c) => (c.flags & 0b1) != 0)
      .map((c) => ({ name: c.name, typeId: c.typeOid }) satisfies storage.ColumnDescriptor);
  }
}
export function getRelId(source: PgoutputRelation): number {
  // Source types are wrong here
  const relId = (source as any).relationOid as number;
  if (!relId) {
    throw new ReplicationAssertionError(`No relation id found`);
  }
  return relId;
}

export function getPgOutputRelation(source: PgoutputRelation): storage.SourceEntityDescriptor {
  return {
    name: source.name,
    schema: source.schema,
    objectId: getRelId(source),
    replicaIdColumns: getReplicaIdColumns(source)
  } satisfies storage.SourceEntityDescriptor;
}

export function referencedColumnTypeIds(source: PgoutputRelation): number[] {
  const oids = new Set<number>();
  for (const column of source.columns) {
    oids.add(column.typeOid);
  }

  return [...oids];
}
