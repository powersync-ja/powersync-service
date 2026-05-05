import { v } from 'convex/values';
import { mutation, query } from './_generated/server.js';
import schema from './schema.js';

const BATCH_SIZE = 4000;

export const get = query({
  args: {},
  handler: async (ctx) => {
    return await ctx.db.query('lists').collect();
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
    const lists = await ctx.db.query('lists').take(batchSize);
    for (const list of lists) {
      await ctx.db.delete(list._id);
    }
    return lists.length;
  }
});

export const createBatch = mutation({
  args: {
    lists: v.array(schema.tables.lists.validator)
  },
  handler: async (ctx, args) => {
    const ids = [];
    for (const list of args.lists) {
      const id = await ctx.db.insert('lists', list);
      ids.push(id);
    }
    return ids;
  }
});
