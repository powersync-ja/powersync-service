import { CONVEX_CHECKPOINT_TABLE } from '../common/ConvexCheckpoints.js';
import { NormalizedConvexConnectionConfig } from '../types/types.js';
import { ConvexConnectionManager } from './ConvexConnectionManager.js';

const MISSING_MUTATOR_FRAGMENT = `
Define a mutator for the PowerSync service to use

convex/powersync_checkpoints.ts
\`\`\`TypeScript
import { mutation } from './_generated/server.js';

export const createCheckpoint = mutation({
    args: {},
    handler: async (ctx) => {
    const existing = await ctx.db.query('powersync_checkpoints').first();

    if (existing) {
        await ctx.db.patch(existing._id, { last_updated: Date.now() });
    } else {
        await ctx.db.insert('powersync_checkpoints', { last_updated: Date.now() });
    }
    }
});
\`\`\``.trim();

export type ConvexSourceConfigurationTestResult = {
  connected: boolean;
  errors: string[];
};

export type CheckSourceConfigurationOptions = {
  /**
   * Avoid calling Convex mutations while checking the source configuration.
   *
   * This is used by diagnostics and connection-status routes, which may be
   * called often. Creating write checkpoint markers there is safe, but
   * unnecessary and can create noisy replication activity on every poll.
   */
  readOnly?: boolean;
};

export async function checkSourceConfiguration(
  normalizedConfig: NormalizedConvexConnectionConfig,
  options: CheckSourceConfigurationOptions = {}
): Promise<ConvexSourceConfigurationTestResult> {
  const connectionManager = new ConvexConnectionManager(normalizedConfig);
  const errors: string[] = [];
  let connected = false;
  try {
    // Check if the database is reachable by fetching the schema.
    const schema = await connectionManager.client.getJsonSchemas().catch((error) => {
      const message = error instanceof Error ? error.message : `${error}`;
      errors.push(
        `Could not fetch Convex schema for provided connection configuration. Error: ${message || 'unknown'}`
      );
    });

    // We could connect if we got a schema response
    connected = !!schema;
    if (!schema) {
      return {
        connected,
        errors
      };
    }

    const hasCheckpointTable = schema.tables.some((table) => table.tableName == CONVEX_CHECKPOINT_TABLE);
    if (!hasCheckpointTable) {
      errors.push(
        `
Could not find the ${CONVEX_CHECKPOINT_TABLE} table in the schema.

Define the ${CONVEX_CHECKPOINT_TABLE} table in the Convex schema:

convex/schema.ts
\`\`\`TypeScript
//...

export default defineSchema({
    // ... your other tables

    powersync_checkpoints: defineTable({
    last_updated: v.float64()
    })
});
\`\`\`

${MISSING_MUTATOR_FRAGMENT}
    `.trim()
      );
    }

    // Check that the PowerSync checkpoint table and mutation are deployed.
    // It should be safe to update this table at any point. We only use it for emitting a replication event.
    if (hasCheckpointTable && !options.readOnly) {
      await connectionManager.client.createWriteCheckpointMarker().catch((error) => {
        const message = error instanceof Error ? error.message : `${error}`;
        errors.push(
          `
Could not call the createCheckpoint mutator. Error ${message || 'unknown'}

${MISSING_MUTATOR_FRAGMENT}`.trim()
        );
      });
    }
  } finally {
    await connectionManager.end();
  }
  return {
    connected,
    errors
  };
}
