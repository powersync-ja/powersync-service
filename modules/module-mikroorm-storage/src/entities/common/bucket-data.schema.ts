import { defineEntity, p, type InferEntity } from '@mikro-orm/core';

export const BucketDataSchema = defineEntity({
  name: 'BucketData',
  tableName: 'bucket_data',
  indexes: [
    {
      name: 'bucket_data_bucket_op_index',
      properties: ['groupId', 'bucketName', 'opId']
    },
    {
      name: 'bucket_data_source_index',
      properties: ['groupId', 'sourceTable', 'sourceKey']
    }
  ],
  properties: {
    id: p.string().primary(),
    groupId: p.integer().fieldName('group_id'),
    bucketName: p.string().fieldName('bucket_name'),
    opId: p.bigint('bigint'),
    op: p.string(),
    sourceTable: p.string().fieldName('source_table').nullable(),
    sourceKey: p.blob().nullable(),
    tableName: p.string().fieldName('table_name').nullable(),
    rowId: p.string().fieldName('row_id').nullable(),
    checksum: p.bigint('bigint'),
    data: p.text().nullable(),
    targetOp: p.bigint('bigint').nullable()
  }
});

export const BucketData = BucketDataSchema.class;
export type BucketData = InferEntity<typeof BucketDataSchema>;
