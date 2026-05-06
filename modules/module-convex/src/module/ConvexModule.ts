import {
  api,
  ConfigurationFileSyncRulesProvider,
  ConnectionTestResult,
  replication,
  system,
  TearDownOptions
} from '@powersync/service-core';
import { ConvexRouteAPIAdapter } from '../api/ConvexRouteAPIAdapter.js';
import { CONVEX_CHECKPOINT_TABLE } from '../common/ConvexCheckpoints.js';
import { ConvexConnectionManager } from '../replication/ConvexConnectionManager.js';
import { ConvexConnectionManagerFactory } from '../replication/ConvexConnectionManagerFactory.js';
import { ConvexErrorRateLimiter } from '../replication/ConvexErrorRateLimiter.js';
import { ConvexReplicator } from '../replication/ConvexReplicator.js';
import * as types from '../types/types.js';

export class ConvexModule extends replication.ReplicationModule<types.ConvexConnectionConfig> {
  constructor() {
    super({
      name: 'Convex',
      type: types.CONVEX_CONNECTION_TYPE,
      configSchema: types.ConvexConnectionConfig
    });
  }

  async onInitialized(context: system.ServiceContextContainer): Promise<void> {}

  protected createRouteAPIAdapter(): api.RouteAPI {
    return new ConvexRouteAPIAdapter(this.resolveConfig(this.decodedConfig!));
  }

  protected createReplicator(context: system.ServiceContext): replication.AbstractReplicator {
    const normalizedConfig = this.resolveConfig(this.decodedConfig!);
    const syncRuleProvider = new ConfigurationFileSyncRulesProvider(context.configuration.sync_rules);
    const connectionFactory = new ConvexConnectionManagerFactory(normalizedConfig);

    return new ConvexReplicator({
      id: this.getDefaultId(normalizedConfig.deployment_url),
      syncRuleProvider,
      storageEngine: context.storageEngine,
      metricsEngine: context.metricsEngine,
      connectionFactory,
      rateLimiter: new ConvexErrorRateLimiter()
    });
  }

  private resolveConfig(config: types.ConvexConnectionConfig): types.ResolvedConvexConnectionConfig {
    return {
      ...config,
      ...types.normalizeConnectionConfig(config)
    };
  }

  async teardown(options: TearDownOptions): Promise<void> {
    // No source-side teardown required.
  }

  async testConnection(config: types.ConvexConnectionConfig) {
    this.decodeConfig(config);
    const normalizedConfig = this.resolveConfig(this.decodedConfig!);
    return await ConvexModule.testConnection(normalizedConfig);
  }

  static async testConnection(normalizedConfig: types.ResolvedConvexConnectionConfig): Promise<ConnectionTestResult> {
    const connectionManager = new ConvexConnectionManager(normalizedConfig);
    const missingMutatorErrorFragment = `
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
\`\`\`  
  `;
    try {
      // Check if the database is reachable by fetching the schema.
      const schema = await connectionManager.client.getJsonSchemas().catch((error) => {
        throw new Error('Could not fetch Convex schema for provided connection configuration.', {
          cause: error
        });
      });

      if (!schema.tables.find((table) => table.tableName == CONVEX_CHECKPOINT_TABLE)) {
        throw new Error(`
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

${missingMutatorErrorFragment}
`);
      }

      // Check that the PowerSync checkpoint table and mutation are deployed.
      // It should be safe to update this table at any point. We only use it for emiting a replication event.
      await connectionManager.client.createWriteCheckpointMarker().catch((error) => {
        throw new Error(
          `
Could not call the createCheckpoint mutator.

${missingMutatorErrorFragment}
          `,
          { cause: error }
        );
      });
    } finally {
      await connectionManager.end();
    }

    return {
      connectionDescription: normalizedConfig.deployment_url
    };
  }
}
