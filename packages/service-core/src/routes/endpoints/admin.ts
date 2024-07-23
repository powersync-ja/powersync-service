import { errors, router, schema } from '@powersync/lib-services-framework';
import { SqlSyncRules, SqliteValue, StaticSchema, isJsonValue } from '@powersync/service-sync-rules';
import { internal_routes } from '@powersync/service-types';

import * as api from '../../api/api-index.js';

import { routeDefinition } from '../router.js';
import { PersistedSyncRulesContent } from '../../storage/BucketStorage.js';
import { authApi } from '../auth.js';

const demoCredentials = routeDefinition({
  path: '/api/admin/v1/demo-credentials',
  method: router.HTTPMethod.POST,
  authorize: authApi,
  validator: schema.createTsCodecValidator(internal_routes.DemoCredentialsRequest, {
    allowAdditional: true
  }),
  handler: async (payload) => {
    // TODO is this used?
    // const connection = payload.context.system.config.connection;
    // if (connection == null || !connection.demo_database) {
    //   return internal_routes.DemoCredentialsResponse.encode({});
    // }
    // const uri = util.buildDemoPgUri(connection);
    // return internal_routes.DemoCredentialsResponse.encode({
    //   credentials: {
    //     postgres_uri: uri
    //   }
    // });
  }
});

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

    const api = payload.context.service_context.syncAPIProvider.getSyncAPI();
    const sourceConfig = await api?.getSourceConfig();
    if (!sourceConfig?.debug_enabled) {
      return internal_routes.ExecuteSqlResponse.encode({
        results: {
          columns: [],
          rows: []
        },
        success: false,
        error: 'SQL querying is not enabled'
      });
    }

    return internal_routes.ExecuteSqlResponse.encode(await api!.executeQuery(query, args));
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

    const syncAPI = service_context.syncAPIProvider.getSyncAPI();
    const status = await syncAPI?.getConnectionStatus();
    if (!status) {
      return internal_routes.DiagnosticsResponse.encode({
        connections: []
      });
    }

    const { storage } = service_context;
    const active = await storage.getActiveSyncRulesContent();
    const next = await storage.getNextSyncRulesContent();

    const active_status = await api.getSyncRulesStatus(active, {
      include_content,
      check_connection: status.connected,
      live_status: true
    });

    const next_status = await api.getSyncRulesStatus(next, {
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
  handler: async () => {
    return internal_routes.GetSchemaResponse.encode(await api.getConnectionsSchema());
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
    const storage = service_context.storage;
    const next = await storage.getNextSyncRules();
    if (next != null) {
      throw new Error(`Busy processing sync rules - cannot reprocess`);
    }

    const active = await storage.getActiveSyncRules();
    if (active == null) {
      throw new errors.JourneyError({
        status: 422,
        code: 'NO_SYNC_RULES',
        description: 'No active sync rules'
      });
    }

    const new_rules = await storage.updateSyncRules({
      content: active.sync_rules.content
    });

    const api = service_context.syncAPIProvider.getSyncAPI();
    const baseConfig = await api?.getSourceConfig();

    return internal_routes.ReprocessResponse.encode({
      connections: [
        {
          // Previously the connection was asserted with `!`
          tag: baseConfig!.tag!,
          id: baseConfig!.id,
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

    const schemaData = await api.getConnectionsSchema();
    const schema = new StaticSchema(schemaData.connections);

    const sync_rules: PersistedSyncRulesContent = {
      // Dummy values
      id: 0,
      slot_name: '',

      parsed() {
        return {
          ...this,
          sync_rules: SqlSyncRules.fromYaml(content, { throwOnError: false, schema })
        };
      },
      sync_rules_content: content,
      async lock() {
        throw new Error('Lock not implemented');
      }
    };

    const apiHandler = service_context.syncAPIProvider.getSyncAPI();
    const connectionStatus = await apiHandler?.getConnectionStatus();
    if (!connectionStatus) {
      return internal_routes.ValidateResponse.encode({
        errors: [{ level: 'fatal', message: 'No connection configured' }],
        connections: []
      });
    }

    const status = (await api.getSyncRulesStatus(sync_rules, {
      include_content: false,
      check_connection: connectionStatus.connected,
      live_status: false
    }))!;

    if (connectionStatus == null) {
      status.errors.push({ level: 'fatal', message: 'No connection configured' });
    }

    return internal_routes.ValidateResponse.encode(status);
  }
});

function mapColumnValue(value: SqliteValue) {
  if (typeof value == 'bigint') {
    return Number(value);
  } else if (isJsonValue(value)) {
    return value;
  } else {
    return null;
  }
}

export const ADMIN_ROUTES = [demoCredentials, executeSql, diagnostics, getSchema, reprocess, validate];
