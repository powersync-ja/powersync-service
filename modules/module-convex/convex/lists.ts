import { v } from 'convex/values';
import { mutation, query } from './_generated/server.js';
import schema from './schema.js';
import { findListByUuid } from './utils/collections.js';

const BATCH_SIZE = 2000;

export const get = query({
  args: {},
  handler: async (ctx) => {
    return await ctx.db.query('lists').collect();
  }
});

export const deleteItem = mutation({
  args: {
    uuid: v.string()
  },
  handler: async ({ db }, { uuid }) => {
    const found = await findListByUuid({ db: db, uuid });
    if (!found) {
      throw new Error('Not found');
    }
    await db.delete('lists', found._id);
  }
});

export const updateName = mutation({
  args: {
    uuid: v.string(),
    name: v.string()
  },
  handler: async ({ db }, { uuid, name }) => {
    const found = await findListByUuid({ db, uuid });
    if (!found) {
      throw new Error('Not found');
    }
    await db.patch(found._id, { name });
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

/**
 * A test mutation which creates and updates multiple lists multiple times.
 * This is used to ensure the order of ops is safely replicated.
 */
export const testUpdateMultipleTimes = mutation({
  args: {},
  handler: async (ctx) => {
    const listCount = 5;
    const createdListIds = [];
    for (let i = 0; i < listCount; i++) {
      const id = await ctx.db.insert('lists', {
        name: `list-${i}`,
        uuid: 'fake-uuid'
      });
      createdListIds.push(id);
    }

    // Update all the lists
    for (let i = 0; i < listCount; i++) {
      await ctx.db.patch('lists', createdListIds[i]!, {
        name: `list-${i}-a`,
        uuid: createdListIds[i]! // keep this for later
      });
    }

    // Do a second update, but operate on the items in reverse order
    for (let i = listCount - 1; i >= 0; i--) {
      await ctx.db.patch('lists', createdListIds[i]!, {
        name: `list-${i}-a-b`
      });
    }

    for (let i = 0; i < listCount; i++) {
      await ctx.db.patch('lists', createdListIds[i]!, {
        name: `list-${i}-a-b-c`
      });
    }

    // delete a random list
    const [deleteId] = createdListIds.splice(Math.floor(Math.random() * createdListIds.length), 1);
    await ctx.db.delete('lists', deleteId);

    return {
      listIds: createdListIds
    };
  }
});
