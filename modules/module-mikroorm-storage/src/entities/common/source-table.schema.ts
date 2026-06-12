import { defineEntity, p, type InferEntity } from '@mikro-orm/core';

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

export const SourceTable = SourceTableSchema.class;
export type SourceTable = InferEntity<typeof SourceTableSchema>;
