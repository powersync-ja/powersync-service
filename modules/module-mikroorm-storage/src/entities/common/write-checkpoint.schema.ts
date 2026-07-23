import { defineEntity, p, type InferEntity } from '@mikro-orm/core';

export const WriteCheckpointSchema = defineEntity({
  name: 'WriteCheckpoint',
  tableName: 'write_checkpoints',
  indexes: [
    {
      name: 'write_checkpoints_user_checkpoint_index',
      properties: ['userId', 'syncRulesId', 'checkpoint']
    }
  ],
  properties: {
    id: p.string().primary(),
    syncRulesId: p.integer().fieldName('sync_rules_id').nullable(),
    userId: p.string().fieldName('user_id'),
    checkpoint: p.bigint('bigint'),
    heads: p.json().nullable(),
    createdAt: p.datetime()
  }
});

export const WriteCheckpoint = WriteCheckpointSchema.class;
export type WriteCheckpoint = InferEntity<typeof WriteCheckpointSchema>;
