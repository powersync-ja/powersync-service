import { defineEntity, p, type InferEntity } from '@mikro-orm/core';

export const CurrentDataSchema = defineEntity({
  name: 'CurrentData',
  tableName: 'current_data',
  indexes: [
    {
      name: 'current_data_source_index',
      properties: ['groupId', 'sourceTable', 'sourceKey']
    },
    {
      name: 'current_data_pending_delete_index',
      properties: ['groupId', 'pendingDelete']
    }
  ],
  properties: {
    id: p.string().primary(),
    groupId: p.integer().fieldName('group_id'),
    sourceTable: p.string().fieldName('source_table'),
    sourceKey: p.blob(),
    buckets: p.json(),
    lookups: p.json(),
    data: p.blob(),
    pendingDelete: p.bigint('bigint').nullable()
  }
});

export const CurrentData = CurrentDataSchema.class;
export type CurrentData = InferEntity<typeof CurrentDataSchema>;
