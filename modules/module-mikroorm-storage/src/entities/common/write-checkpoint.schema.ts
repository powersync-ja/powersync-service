import { defineEntity, p } from '@mikro-orm/core';

export class WriteCheckpoint {
  declare id: string;
  declare syncRulesId: number | null;
  declare userId: string;
  declare checkpoint: bigint;
  declare heads: unknown | null;
  declare createdAt: Date;
}

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
WriteCheckpointSchema.setClass(WriteCheckpoint);
