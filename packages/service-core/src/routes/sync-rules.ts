import * as t from 'ts-codec';
import { FastifyPluginAsync, FastifyReply } from 'fastify';
import * as micro from '@journeyapps-platform/micro';
import * as pgwire from '@powersync/service-jpgwire';
import { SqlSyncRules, SyncRulesErrors } from '@powersync/service-sync-rules';

import * as replication from '../replication/replication-index.js';
import { authApi } from './auth.js';
import { RouteGenerator } from './router.js';

const DeploySyncRulesRequest = t.object({
  content: t.string
});

const yamlPlugin: FastifyPluginAsync = async (fastify) => {
  fastify.addContentTypeParser('application/yaml', async (request, payload, _d) => {
    const data = await micro.streaming.drain(payload);

    request.params = { content: Buffer.concat(data).toString('utf8') };
  });
};

export const deploySyncRules: RouteGenerator = (router) =>
  router.post('/api/sync-rules/v1/deploy', {
    authorize: authApi,
    parse: true,
    plugins: [yamlPlugin],
    validator: micro.schema.createTsCodecValidator(DeploySyncRulesRequest, { allowAdditional: true }),
    handler: async (payload) => {
      if (payload.context.system.config.sync_rules.present) {
        // If sync rules are configured via the config, disable deploy via the API.
        throw new micro.errors.JourneyError({
          status: 422,
          code: 'API_DISABLED',
          description: 'Sync rules API disabled',
          details: 'Use the management API to deploy sync rules'
        });
      }
      const content = payload.params.content;

      try {
        SqlSyncRules.fromYaml(payload.params.content);
      } catch (e) {
        throw new micro.errors.JourneyError({
          status: 422,
          code: 'INVALID_SYNC_RULES',
          description: 'Sync rules parsing failed',
          details: e.message
        });
      }

      const sync_rules = await payload.context.system.storage.updateSyncRules({
        content: content
      });

      return {
        slot_name: sync_rules.slot_name
      };
    }
  });

const ValidateSyncRulesRequest = t.object({
  content: t.string
});

export const validateSyncRules: RouteGenerator = (router) =>
  router.post('/api/sync-rules/v1/validate', {
    authorize: authApi,
    parse: true,
    plugins: [yamlPlugin],
    validator: micro.schema.createTsCodecValidator(ValidateSyncRulesRequest, { allowAdditional: true }),
    handler: async (payload) => {
      const content = payload.params.content;

      const info = await debugSyncRules(payload.context.system.requirePgPool(), content);

      replyPrettyJson(payload.reply, info);
    }
  });

export const currentSyncRules: RouteGenerator = (router) =>
  router.get('/api/sync-rules/v1/current', {
    authorize: authApi,
    handler: async (payload) => {
      const storage = payload.context.system.storage;
      const sync_rules = await storage.getActiveSyncRulesContent();
      if (!sync_rules) {
        throw new micro.errors.JourneyError({
          status: 422,
          code: 'NO_SYNC_RULES',
          description: 'No active sync rules'
        });
      }
      const info = await debugSyncRules(payload.context.system.requirePgPool(), sync_rules.sync_rules_content);
      const next = await storage.getNextSyncRulesContent();

      const next_info = next
        ? await debugSyncRules(payload.context.system.requirePgPool(), next.sync_rules_content)
        : null;

      const response = {
        current: {
          slot_name: sync_rules.slot_name,
          content: sync_rules.sync_rules_content,
          ...info
        },
        next:
          next == null
            ? null
            : {
                slot_name: next.slot_name,
                content: next.sync_rules_content,
                ...next_info
              }
      };
      replyPrettyJson(payload.reply, { data: response });
    }
  });

const ReprocessSyncRulesRequest = t.object({});

export const reprocessSyncRules: RouteGenerator = (router) =>
  router.post('/api/sync-rules/v1/reprocess', {
    authorize: authApi,
    validator: micro.schema.createTsCodecValidator(ReprocessSyncRulesRequest),
    handler: async (payload) => {
      const storage = payload.context.system.storage;
      const sync_rules = await storage.getActiveSyncRules();
      if (sync_rules == null) {
        throw new micro.errors.JourneyError({
          status: 422,
          code: 'NO_SYNC_RULES',
          description: 'No active sync rules'
        });
      }

      const new_rules = await storage.updateSyncRules({
        content: sync_rules.sync_rules.content
      });
      return {
        slot_name: new_rules.slot_name
      };
    }
  });

export const syncRulesRoutes = [validateSyncRules, deploySyncRules, reprocessSyncRules, currentSyncRules];

function replyPrettyJson(reply: FastifyReply, payload: any) {
  reply
    .status(200)
    .header('Content-Type', 'application/json')
    .send(JSON.stringify(payload, null, 2) + '\n');
}

async function debugSyncRules(db: pgwire.PgClient, sync_rules: string) {
  try {
    const rules = SqlSyncRules.fromYaml(sync_rules);
    const source_table_patterns = rules.getSourceTables();
    const wc = new replication.WalConnection({
      db: db,
      sync_rules: rules
    });
    const resolved_tables = await wc.getDebugTablesInfo(source_table_patterns);

    return {
      valid: true,
      bucket_definitions: rules.bucket_descriptors.map((d) => {
        let all_parameter_queries = [...d.parameter_queries.values()].flat();
        let all_data_queries = [...d.data_queries.values()].flat();
        return {
          name: d.name,
          bucket_parameters: d.bucket_parameters,
          global_parameter_queries: d.global_parameter_queries.map((q) => {
            return {
              sql: q.sql
            };
          }),
          parameter_queries: all_parameter_queries.map((q) => {
            return {
              sql: q.sql,
              table: q.sourceTable,
              input_parameters: q.input_parameters
            };
          }),

          data_queries: all_data_queries.map((q) => {
            return {
              sql: q.sql,
              table: q.sourceTable,
              columns: q.columnOutputNames()
            };
          })
        };
      }),
      source_tables: resolved_tables,
      data_tables: rules.debugGetOutputTables()
    };
  } catch (e) {
    if (e instanceof SyncRulesErrors) {
      return {
        valid: false,
        errors: e.errors.map((e) => e.message)
      };
    }
    return {
      valid: false,
      errors: [e.message]
    };
  }
}
