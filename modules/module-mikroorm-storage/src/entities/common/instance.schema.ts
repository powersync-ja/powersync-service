import { defineEntity, p, type InferEntity } from '@mikro-orm/core';

export const InstanceSchema = defineEntity({
  name: 'Instance',
  tableName: 'instance',
  properties: {
    id: p.string().primary()
  }
});

export const Instance = InstanceSchema.class;
export type Instance = InferEntity<typeof InstanceSchema>;
