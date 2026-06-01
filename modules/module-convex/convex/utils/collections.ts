import { DatabaseReader } from '../_generated/server.js';

export const findListByUuid = async (params: { db: DatabaseReader; uuid: string }) => {
  const { db, uuid } = params;
  return db
    .query('lists')
    .withIndex('by_uuid', (q) => q.eq('uuid', uuid))
    .first();
};

export const findTodoByUuid = async (params: { db: DatabaseReader; uuid: string }) => {
  const { db, uuid } = params;
  return db
    .query('todos')
    .withIndex('by_uuid', (q) => q.eq('uuid', uuid))
    .first();
};
