import type { Opt } from '@mikro-orm/core';
import { defineEntity, p } from '@mikro-orm/core';

export class SourceTable {
  declare id: string;
  declare groupId: number;
  declare connectionId: number;
  declare relationId: unknown | null;
  declare schemaName: string;
  declare tableName: string;
  declare replicaIdColumns: unknown | null;
  declare snapshotDone: Opt<boolean>;
  declare snapshotTotalEstimatedCount: bigint | null;
  declare snapshotReplicatedCount: bigint | null;
  declare snapshotLastKey: Buffer | Uint8Array | null;
}

export const SourceTableSchema = defineEntity({
  name: 'SourceTable',
  tableName: 'source_tables',
  indexes: [
    {
      name: 'source_table_lookup',
      properties: ['groupId', 'tableName']
    }
  ],
  properties: {
    id: p.string().primary(),
    groupId: p.integer().fieldName('group_id'),
    connectionId: p.integer().fieldName('connection_id'),
    relationId: p.json().nullable(),
    schemaName: p.string().fieldName('schema_name'),
    tableName: p.string().fieldName('table_name'),
    replicaIdColumns: p.json().nullable(),
    snapshotDone: p.boolean().fieldName('snapshot_done').default(true),
    snapshotTotalEstimatedCount: p.bigint('bigint').nullable(),
    snapshotReplicatedCount: p.bigint('bigint').nullable(),
    snapshotLastKey: p.blob().nullable()
  }
});
SourceTableSchema.setClass(SourceTable);
