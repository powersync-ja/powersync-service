import { defineEntity, p, type InferEntity } from '@mikro-orm/core';

export const BucketParametersSchema = defineEntity({
  name: 'BucketParameters',
  tableName: 'bucket_parameters',
  indexes: [
    {
      name: 'bucket_parameters_lookup_index',
      properties: ['groupId', 'lookup', 'id']
    },
    {
      name: 'bucket_parameters_source_index',
      properties: ['groupId', 'sourceTable', 'sourceKey']
    }
  ],
  properties: {
    id: p.bigint('bigint').primary().autoincrement(false),
    groupId: p.integer().fieldName('group_id'),
    sourceTable: p.string().fieldName('source_table'),
    sourceKey: p.blob(),
    lookup: p.blob(),
    bucketParameters: p.json()
  }
});

export const BucketParameters = BucketParametersSchema.class;
export type BucketParameters = InferEntity<typeof BucketParametersSchema>;
