import { ErrorCode, errors, router, schema } from '@powersync/lib-services-framework';
import { SqlSyncRules, SyncRulesErrors } from '@powersync/service-sync-rules';
import type { FastifyPluginAsync } from 'fastify';
import * as t from 'ts-codec';

import { RouteAPI } from '../../api/RouteAPI.js';
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
    const { storageEngine } = service_context;

    if (service_context.configuration.sync_rules.present) {
      // If sync rules are configured via the config, disable deploy via the API.
      throw new errors.ServiceError({
        status: 422,
        code: ErrorCode.PSYNC_S4105,
        description: 'Sync rules API disabled',
        details: 'Use the management API to deploy sync rules'
      });
    }
    const content = payload.params.content;

    try {
      const apiHandler = service_context.routerEngine.getAPI();
      SqlSyncRules.fromYaml(payload.params.content, {
        ...apiHandler.getParseSyncRulesOptions(),
        // We don't do any schema-level validation at this point
        schema: undefined
      });
    } catch (e) {
      throw new errors.ServiceError({
        status: 422,
        code: ErrorCode.PSYNC_R0001,
        description: 'Sync rules parsing failed',
        details: e.message
      });
    }

    const sync_rules = await storageEngine.activeBucketStorage.updateSyncRules({
      content: content,
      // Aready validated above
      validate: false
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
    const { service_context } = payload.context;
    const apiHandler = service_context.routerEngine.getAPI();

    const info = await debugSyncRules(apiHandler, content);

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
      storageEngine: { activeBucketStorage }
    } = service_context;

    const sync_rules = await activeBucketStorage.getActiveSyncRulesContent();
    if (!sync_rules) {
      throw new errors.ServiceError({
        status: 422,
        code: ErrorCode.PSYNC_S4104,
        description: 'No active sync rules'
      });
    }

    const apiHandler = service_context.routerEngine.getAPI();
    const info = await debugSyncRules(apiHandler, sync_rules.sync_rules_content);
    const next = await activeBucketStorage.getNextSyncRulesContent();

    const next_info = next ? await debugSyncRules(apiHandler, next.sync_rules_content) : null;

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
      storageEngine: { activeBucketStorage }
    } = payload.context.service_context;
    const apiHandler = payload.context.service_context.routerEngine.getAPI();
    const sync_rules = await activeBucketStorage.getActiveSyncRules(apiHandler.getParseSyncRulesOptions());
    if (sync_rules == null) {
      throw new errors.ServiceError({
        status: 422,
        code: ErrorCode.PSYNC_S4104,
        description: 'No active sync rules'
      });
    }

    const new_rules = await activeBucketStorage.updateSyncRules({
      content: sync_rules.sync_rules.content,
      // These sync rules already passed validation. But if the rules are not valid anymore due
      // to a service change, we do want to report the error here.
      validate: true
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

async function debugSyncRules(apiHandler: RouteAPI, sync_rules: string) {
  try {
    const rules = SqlSyncRules.fromYaml(sync_rules, {
      ...apiHandler.getParseSyncRulesOptions(),
      // No schema-based validation at this point
      schema: undefined
    });
    const source_table_patterns = rules.getSourceTables();
    const resolved_tables = await apiHandler.getDebugTablesInfo(source_table_patterns, rules);

    return {
      valid: true,
      bucket_definitions: rules.bucketSources.map((source) => source.debugRepresentation()),
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
