import { v } from 'convex/values';
import { mutation } from './_generated/server.js';

export const BENCHMARK_LIST_PAYLOAD = {
  uuid: 'benchmark-list',
  name: 'Benchmark list'
} as const;

export const BENCHMARK_TODO_PAYLOAD = {
  uuid: 'benchmark-todo',
  description: 'Benchmark todo',
  list_uuid: BENCHMARK_LIST_PAYLOAD.uuid
} as const;

/**
 * Creates seed data for sync replication benchmarking purposes.
 *
 * Logical payload estimate for the current row shape is calculated in the benchmark test from
 * these payload constants. Each loop creates one list and one todo, so the estimate is:
 * rowsPerTable * (encoded JSON list bytes + encoded JSON todo bytes + representative list_id bytes).
 * Convex write-limit accounting can be higher because it also includes document metadata,
 * indexes and storage internals.
 */
export const seedInitialReplicationBatch = mutation({
  args: {
    rowsPerTable: v.number()
  },
  handler: async (ctx, args) => {
    for (let i = 0; i < args.rowsPerTable; i++) {
      const listId = await ctx.db.insert('lists', {
        ...BENCHMARK_LIST_PAYLOAD
      });

      await ctx.db.insert('todos', {
        ...BENCHMARK_TODO_PAYLOAD,
        list_id: listId
      });
    }

    return {
      lists: args.rowsPerTable,
      todos: args.rowsPerTable
    };
  }
});
