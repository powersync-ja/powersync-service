import { defineEntity, p } from '@mikro-orm/core';

export class BucketParameters {
  declare id: bigint;
  declare groupId: number;
  declare sourceTable: string;
  declare sourceKey: Buffer | Uint8Array;
  declare lookup: Buffer | Uint8Array;
  declare bucketParameters: unknown;
}

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
BucketParametersSchema.setClass(BucketParameters);
