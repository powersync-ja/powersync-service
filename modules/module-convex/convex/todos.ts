import { v } from 'convex/values';
import { mutation, query } from './_generated/server.js';
import schema from './schema.js';
import { findListByUuid } from './utils/collections.js';

const BATCH_SIZE = 4000;

export const get = query({
  args: {},
  handler: async (ctx) => {
    return await ctx.db.query('todos').collect();
  }
});

/**
 * Deletes a batch of items.
 * Convex limits the number of ops in a mutation/transaction to 16_000.
 * This will return the number of items deleted.
 * Rerun this till completion if all items should be deleted.
 */
export const deleteBatch = mutation({
  args: {
    batch_size: v.optional(v.number())
  },
  handler: async (ctx, args) => {
    const batchSize = args.batch_size ?? BATCH_SIZE;
    const todos = await ctx.db.query('todos').take(batchSize);
    for (const todo of todos) {
      await ctx.db.delete(todo._id);
    }
    return todos.length;
  }
});

export const createBatch = mutation({
  args: {
    // We don't require clients usually to track the Convex list_id, they work with list_uuid
    todos: v.array(schema.tables.todos.validator.omit('list_id'))
  },
  handler: async (ctx, args) => {
    const { db } = ctx;
    const ids = [];
    for (const todo of args.todos) {
      const list = await findListByUuid({ db, uuid: todo.list_uuid });
      if (!list) {
        // Continue for simple testing purposes
        continue;
      }
      const id = await ctx.db.insert('todos', {
        ...todo,
        list_id: list._id
      });

      ids.push(id);
    }
    return ids;
  }
});
