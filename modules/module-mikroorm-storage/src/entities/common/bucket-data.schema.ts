import { defineEntity, p } from '@mikro-orm/core';

export class BucketData {
  declare id: string;
  declare groupId: number;
  declare bucketName: string;
  declare opId: bigint;
  declare op: string;
  declare sourceTable: string | null;
  declare sourceKey: Buffer | Uint8Array | null;
  declare tableName: string | null;
  declare rowId: string | null;
  declare checksum: bigint;
  declare data: string | null;
  declare targetOp: bigint | null;
}

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
    data: p.string().nullable(),
    targetOp: p.bigint('bigint').nullable()
  }
});
BucketDataSchema.setClass(BucketData);
