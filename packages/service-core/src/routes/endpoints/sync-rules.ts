import { errors, router, schema } from '@powersync/lib-services-framework';
import { SqlSyncRules, SyncRulesErrors } from '@powersync/service-sync-rules';
import type { FastifyPluginAsync } from 'fastify';
import * as t from 'ts-codec';

import * as system from '../../system/system-index.js';
import { authApi } from '../auth.js';
import { routeDefinition } from '../router.js';

const DeploySyncRulesRequest = t.object({
  content: t.string
});

export const yamlPlugin: FastifyPluginAsync = async (fastify) => {
  fastify.addContentTypeParser('application/yaml', async (request, payload, _d) => {
    const data: any[] = [];
    for await (const chunk of payload) {
      data.push(chunk);
    }

    request.params = { content: Buffer.concat(data).toString('utf8') };
  });
};

/**
 * Declares the plugin should be available on the same scope
 * without requiring the `fastify-plugin` package as a dependency.
 * https://fastify.dev/docs/latest/Reference/Plugins/#handle-the-scope
 */
//@ts-expect-error
yamlPlugin[Symbol.for('skip-override')] = true;

export const deploySyncRules = routeDefinition({
  path: '/api/sync-rules/v1/deploy',
  method: router.HTTPMethod.POST,
  authorize: authApi,
  parse: true,
  plugins: [yamlPlugin],
  validator: schema.createTsCodecValidator(DeploySyncRulesRequest, { allowAdditional: true }),
  handler: async (payload) => {
    const { service_context } = payload.context;
    const { storage } = service_context;

    if (service_context.configuration.sync_rules.present) {
      // If sync rules are configured via the config, disable deploy via the API.
      throw new errors.JourneyError({
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
      throw new errors.JourneyError({
        status: 422,
        code: 'INVALID_SYNC_RULES',
        description: 'Sync rules parsing failed',
        details: e.message
      });
    }

    const sync_rules = await storage.bucketStorage.updateSyncRules({
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

export const validateSyncRules = routeDefinition({
  path: '/api/sync-rules/v1/validate',
  method: router.HTTPMethod.POST,
  authorize: authApi,
  parse: true,
  plugins: [yamlPlugin],
  validator: schema.createTsCodecValidator(ValidateSyncRulesRequest, { allowAdditional: true }),
  handler: async (payload) => {
    const content = payload.params.content;

    const info = await debugSyncRules(payload.context.service_context, content);

    return replyPrettyJson(info);
  }
});

export const currentSyncRules = routeDefinition({
  path: '/api/sync-rules/v1/current',
  method: router.HTTPMethod.GET,
  authorize: authApi,
  handler: async (payload) => {
    const { service_context } = payload.context;
    const {
      storage: { bucketStorage }
    } = service_context;
    const sync_rules = await bucketStorage.getActiveSyncRulesContent();
    if (!sync_rules) {
      throw new errors.JourneyError({
        status: 422,
        code: 'NO_SYNC_RULES',
        description: 'No active sync rules'
      });
    }
    const info = await debugSyncRules(service_context, sync_rules.sync_rules_content);
    const next = await bucketStorage.getNextSyncRulesContent();

    const next_info = next ? await debugSyncRules(service_context, next.sync_rules_content) : null;

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

    return replyPrettyJson({ data: response });
  }
});

const ReprocessSyncRulesRequest = t.object({});

export const reprocessSyncRules = routeDefinition({
  path: '/api/sync-rules/v1/reprocess',
  method: router.HTTPMethod.POST,
  authorize: authApi,
  validator: schema.createTsCodecValidator(ReprocessSyncRulesRequest),
  handler: async (payload) => {
    const {
      storage: { bucketStorage }
    } = payload.context.service_context;
    const sync_rules = await bucketStorage.getActiveSyncRules();
    if (sync_rules == null) {
      throw new errors.JourneyError({
        status: 422,
        code: 'NO_SYNC_RULES',
        description: 'No active sync rules'
      });
    }

    const new_rules = await bucketStorage.updateSyncRules({
      content: sync_rules.sync_rules.content
    });
    return {
      slot_name: new_rules.slot_name
    };
  }
});

export const SYNC_RULES_ROUTES = [validateSyncRules, deploySyncRules, reprocessSyncRules, currentSyncRules];

function replyPrettyJson(payload: any) {
  return new router.RouterResponse({
    status: 200,
    data: JSON.stringify(payload, null, 2) + '\n',
    headers: { 'Content-Type': 'application/json' }
  });
}

async function debugSyncRules(serviceContext: system.ServiceContext, sync_rules: string) {
  try {
    const rules = SqlSyncRules.fromYaml(sync_rules);
    const source_table_patterns = rules.getSourceTables();

    const api = serviceContext.routerEngine.getAPI();
    if (!api) {
      throw new Error('No API handler found');
    }

    const resolved_tables = await api.getDebugTablesInfo(source_table_patterns, rules);

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
