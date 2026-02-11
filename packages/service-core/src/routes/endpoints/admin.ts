import { ErrorCode, errors, router, schema } from '@powersync/lib-services-framework';
import { SqlSyncRules, StaticSchema } from '@powersync/service-sync-rules';
import { internal_routes } from '@powersync/service-types';

import { DEFAULT_HYDRATION_STATE } from '@powersync/service-sync-rules';
import * as api from '../../api/api-index.js';
import * as storage from '../../storage/storage-index.js';
import { authApi } from '../auth.js';
import { routeDefinition } from '../router.js';

/**
 * @deprecated This will be removed in a future release
 */
export const executeSql = routeDefinition({
  path: '/api/admin/v1/execute-sql',
  method: router.HTTPMethod.POST,
  authorize: authApi,
  validator: schema.createTsCodecValidator(internal_routes.ExecuteSqlRequest, { allowAdditional: true }),
  handler: async (payload) => {
    const {
      params: {
        sql: { query, args }
      }
    } = payload;

    const apiHandler = payload.context.service_context.routerEngine.getAPI();

    const sourceConfig = await apiHandler.getSourceConfig();
    if (!sourceConfig.debug_api) {
      return internal_routes.ExecuteSqlResponse.encode({
        results: {
          columns: [],
          rows: []
        },
        success: false,
        error: 'SQL querying is not enabled'
      });
    }

    return internal_routes.ExecuteSqlResponse.encode(await apiHandler.executeQuery(query, args));
  }
});

export const diagnostics = routeDefinition({
  path: '/api/admin/v1/diagnostics',
  method: router.HTTPMethod.POST,
  authorize: authApi,
  validator: schema.createTsCodecValidator(internal_routes.DiagnosticsRequest, { allowAdditional: true }),
  handler: async (payload) => {
    const { context } = payload;
    const { service_context } = context;
    const include_content = payload.params.sync_rules_content ?? false;

    const apiHandler = service_context.routerEngine.getAPI();

    const status = await apiHandler.getConnectionStatus();
    if (!status) {
      return internal_routes.DiagnosticsResponse.encode({
        connections: []
      });
    }

    const {
      storageEngine: { activeBucketStorage }
    } = service_context;
    const active = await activeBucketStorage.getActiveSyncRulesContent();
    const next = await activeBucketStorage.getNextSyncRulesContent();

    const active_status = await api.getSyncRulesStatus(activeBucketStorage, apiHandler, active, {
      include_content,
      check_connection: status.connected,
      live_status: true
    });

    const next_status = await api.getSyncRulesStatus(activeBucketStorage, apiHandler, next, {
      include_content,
      check_connection: status.connected,
      live_status: true
    });

    return internal_routes.DiagnosticsResponse.encode({
      connections: [
        {
          ...status,
          // TODO update this in future
          postgres_uri: status.uri
        }
      ],
      active_sync_rules: active_status,
      deploying_sync_rules: next_status
    });
  }
});

export const getSchema = routeDefinition({
  path: '/api/admin/v1/schema',
  method: router.HTTPMethod.POST,
  authorize: authApi,
  validator: schema.createTsCodecValidator(internal_routes.GetSchemaRequest, { allowAdditional: true }),
  handler: async (payload) => {
    const apiHandler = payload.context.service_context.routerEngine.getAPI();

    return internal_routes.GetSchemaResponse.encode(await api.getConnectionsSchema(apiHandler));
  }
});

export const reprocess = routeDefinition({
  path: '/api/admin/v1/reprocess',
  method: router.HTTPMethod.POST,
  authorize: authApi,
  validator: schema.createTsCodecValidator(internal_routes.ReprocessRequest, { allowAdditional: true }),
  handler: async (payload) => {
    const {
      context: { service_context }
    } = payload;
    const {
      storageEngine: { activeBucketStorage }
    } = service_context;
    const apiHandler = service_context.routerEngine.getAPI();
    const next = await activeBucketStorage.getNextSyncRules(apiHandler.getParseSyncRulesOptions());
    if (next != null) {
      throw new Error(`Busy processing sync rules - cannot reprocess`);
    }

    const active = await activeBucketStorage.getActiveSyncRules(apiHandler.getParseSyncRulesOptions());
    if (active == null) {
      throw new errors.ServiceError({
        status: 422,
        code: ErrorCode.PSYNC_S4104,
        description: 'No active sync rules'
      });
    }

    const new_rules = await activeBucketStorage.updateSyncRules({
      content: active.sync_rules.config.content,
      // These sync rules already passed validation. But if the rules are not valid anymore due
      // to a service change, we do want to report the error here.
      validate: true
    });

    const baseConfig = await apiHandler.getSourceConfig();

    return internal_routes.ReprocessResponse.encode({
      connections: [
        {
          // Previously the connection was asserted with `!`
          tag: baseConfig.tag,
          id: baseConfig.id,
          slot_name: new_rules.slot_name
        }
      ]
    });
  }
});

export const validate = routeDefinition({
  path: '/api/admin/v1/validate',
  method: router.HTTPMethod.POST,
  authorize: authApi,
  validator: schema.createTsCodecValidator(internal_routes.ValidateRequest, { allowAdditional: true }),
  handler: async (payload) => {
    const {
      context: { service_context }
    } = payload;
    const content = payload.params.sync_rules;
    const apiHandler = service_context.routerEngine.getAPI();

    const schemaData = await api.getConnectionsSchema(apiHandler);
    const schema = new StaticSchema(schemaData.connections);

    const sync_rules: storage.PersistedSyncRulesContent = {
      // Dummy values
      id: 0,
      slot_name: '',
      active: false,
      last_checkpoint_lsn: '',

      parsed() {
        return {
          ...this,
          sync_rules: SqlSyncRules.fromYaml(content, {
            ...apiHandler.getParseSyncRulesOptions(),
            schema
          }),
          hydratedSyncRules() {
            return this.sync_rules.config.hydrate({ hydrationState: DEFAULT_HYDRATION_STATE });
          }
        };
      },
      sync_rules_content: content,
      async lock() {
        throw new Error('Lock not implemented');
      }
    };

    const connectionStatus = await apiHandler.getConnectionStatus();
    if (!connectionStatus) {
      return internal_routes.ValidateResponse.encode({
        errors: [{ level: 'fatal', message: 'No connection configured', ts: new Date().toISOString() }],
        connections: []
      });
    }

    const status = (await api.getSyncRulesStatus(
      service_context.storageEngine.activeBucketStorage,
      apiHandler,
      sync_rules,
      {
        include_content: false,
        check_connection: connectionStatus.connected,
        live_status: false
      }
    ))!;

    if (connectionStatus == null) {
      status.errors.push({ level: 'fatal', message: 'No connection configured', ts: new Date().toISOString() });
    }

    return internal_routes.ValidateResponse.encode(status);
  }
});

export const ADMIN_ROUTES = [executeSql, diagnostics, getSchema, reprocess, validate];
