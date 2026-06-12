import { defineEntity, p } from '@mikro-orm/core';

export class Instance {
  declare id: string;
}

export const InstanceSchema = defineEntity({
  name: 'Instance',
  tableName: 'instance',
  properties: {
    id: p.string().primary()
  }
});
InstanceSchema.setClass(Instance);
