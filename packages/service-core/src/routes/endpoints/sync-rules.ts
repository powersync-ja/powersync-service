import { ErrorCode, errors, router, schema } from '@powersync/lib-services-framework';
import { parseSyncConfig, SyncConfigErrors } from '@powersync/service-sync-rules';
import type { FastifyPluginAsync } from 'fastify';
import * as t from 'ts-codec';
import { assertAcceptedSyncConfig, assertConfiguredSyncConfigValid } from '../../system/SyncConfigEngine.js';

import { PatternResult, RouteAPI } from '../../api/RouteAPI.js';
import { logSyncConfigErrors } from '../../storage/BucketStorageFactory.js';
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
      // If sync config is configured via the service config, disable deploy via the API.
      throw new errors.ServiceError({
        status: 422,
        code: ErrorCode.PSYNC_S4105,
        description: 'Sync config API disabled',
        details: 'Update sync config in the service configuration'
      });
    }
    const content = payload.params.content;
    const compiled = assertAcceptedSyncConfig(await service_context.syncConfigEngine.validate(content));
    logSyncConfigErrors(compiled.parsed);
    const sync_rules = await storageEngine.activeBucketStorage.updateSyncRules({ config: compiled });

    return {
      slot_name: sync_rules.replicationStreamName
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

    const active = await activeBucketStorage.getActiveSyncConfig();
    if (!active) {
      throw new errors.ServiceError({
        status: 422,
        code: ErrorCode.PSYNC_S4104,
        description: 'No active sync config'
      });
    }

    const sync_rules = active.content;
    const apiHandler = service_context.routerEngine.getAPI();
    const info = await debugSyncRules(apiHandler, sync_rules.sync_rules_content);
    const next = await activeBucketStorage.getDeployingSyncConfig();

    const next_info = next ? await debugSyncRules(apiHandler, next.content.sync_rules_content) : null;

    const response = {
      current: {
        slot_name: sync_rules.replicationStreamName,
        content: sync_rules.sync_rules_content,
        ...info
      },
      next:
        next == null
          ? null
          : {
              slot_name: next.content.replicationStreamName,
              content: next.content.sync_rules_content,
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
    const service_context = payload.context.service_context;
    const {
      storageEngine: { activeBucketStorage }
    } = service_context;
    const next = await activeBucketStorage.getDeployingSyncConfig();
    if (next != null) {
      throw new errors.ServiceError({
        status: 409,
        code: ErrorCode.PSYNC_S4106,
        description: 'Busy processing sync config - cannot reprocess'
      });
    }

    await assertConfiguredSyncConfigValid(service_context.syncConfigEngine);

    const active = await activeBucketStorage.getActiveSyncConfig();
    if (active == null) {
      throw new errors.ServiceError({
        status: 422,
        code: ErrorCode.PSYNC_S4104,
        description: 'No active sync config'
      });
    }

    const sync_rules = active.content;
    const compiled = assertAcceptedSyncConfig(
      await service_context.syncConfigEngine.validate(sync_rules.sync_rules_content)
    );

    logSyncConfigErrors(compiled.parsed);
    const new_rules = await activeBucketStorage.updateSyncRules({
      config: compiled,
      version_label: sync_rules.version_label,
      forceNewReplicationStream: true
    });
    return {
      slot_name: new_rules.replicationStreamName
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
    const rules = parseSyncConfig(sync_rules, {
      ...apiHandler.getParseSyncRulesOptions(),
      // No schema-based validation at this point
      schema: undefined
    });
    const errors: string[] = [];
    const warnings = rules.errors.filter((error) => error.type === 'warning').map((error) => error.message);
    const recordError = (error: any) => {
      errors.push(...(error instanceof SyncConfigErrors ? error.errors.map((e) => e.message) : [error.message]));
    };
    try {
      const diagnostics = (await apiHandler.validateSourceCapabilities?.(rules.config)) ?? [];
      for (const diagnostic of diagnostics) {
        (diagnostic.level === 'fatal' ? errors : warnings).push(diagnostic.message);
      }
    } catch (e) {
      recordError(e);
    }
    let resolved_tables: PatternResult[] = [];
    try {
      const source_table_patterns = rules.config.getSourceTables();
      resolved_tables = await apiHandler.getDebugTablesInfo(source_table_patterns, rules.config);
    } catch (e) {
      recordError(e);
    }

    return {
      valid: errors.length == 0,
      ...(warnings.length > 0 ? { warnings } : {}),
      ...(errors.length > 0 ? { errors } : {}),
      bucket_definitions: rules.config.debugRepresentation(),
      source_tables: resolved_tables,
      data_tables: rules.config.debugGetOutputTables()
    };
  } catch (e) {
    if (e instanceof SyncConfigErrors) {
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
