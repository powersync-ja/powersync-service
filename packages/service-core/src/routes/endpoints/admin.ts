import { errors, router, schema } from '@powersync/lib-services-framework';
import { SqlSyncRules, SqliteValue, StaticSchema, isJsonValue, toSyncRulesValue } from '@powersync/service-sync-rules';
import { internal_routes } from '@powersync/service-types';

import * as api from '../../api/api-index.js';
import * as util from '../../util/util-index.js';

import { PersistedSyncRulesContent } from '../../storage/BucketStorage.js';
import { authApi } from '../auth.js';
import { routeDefinition } from '../router.js';

export const executeSql = routeDefinition({
  path: '/api/admin/v1/execute-sql',
  method: router.HTTPMethod.POST,
  authorize: authApi,
  validator: schema.createTsCodecValidator(internal_routes.ExecuteSqlRequest, { allowAdditional: true }),
  handler: async (payload) => {
    const connection = payload.context.system.config.connection;
    if (connection == null || !connection.debug_api) {
      return internal_routes.ExecuteSqlResponse.encode({
        results: {
          columns: [],
          rows: []
        },
        success: false,
        error: 'SQL querying is not enabled. Enable it in your instance settings in order to query your database directly.'
      });
    }

    const pool = payload.context.system.requirePgPool();

    const { query, args } = payload.params.sql;

    try {
      const result = await pool.query({
        statement: query,
        params: args.map(util.autoParameter)
      });

      return internal_routes.ExecuteSqlResponse.encode({
        success: true,
        results: {
          columns: result.columns.map((c) => c.name),
          rows: result.rows.map((row) => {
            return row.map((value) => mapColumnValue(toSyncRulesValue(value)));
          })
        }
      });
    } catch (e) {
      return internal_routes.ExecuteSqlResponse.encode({
        results: {
          columns: [],
          rows: []
        },
        success: false,
        error: e.message
      });
    }
  }
});

export const diagnostics = routeDefinition({
  path: '/api/admin/v1/diagnostics',
  method: router.HTTPMethod.POST,
  authorize: authApi,
  validator: schema.createTsCodecValidator(internal_routes.DiagnosticsRequest, { allowAdditional: true }),
  handler: async (payload) => {
    const include_content = payload.params.sync_rules_content ?? false;
    const system = payload.context.system;

    const status = await api.getConnectionStatus(system);
    if (status == null) {
      return internal_routes.DiagnosticsResponse.encode({
        connections: []
      });
    }

    const { storage } = system;
    const active = await storage.getActiveSyncRulesContent();
    const next = await storage.getNextSyncRulesContent();

    const active_status = await api.getSyncRulesStatus(active, system, {
      include_content,
      check_connection: status.connected,
      live_status: true
    });

    const next_status = await api.getSyncRulesStatus(next, system, {
      include_content,
      check_connection: status.connected,
      live_status: true
    });

    return internal_routes.DiagnosticsResponse.encode({
      connections: [status],
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
    const system = payload.context.system;

    return internal_routes.GetSchemaResponse.encode(await api.getConnectionsSchema(system));
  }
});

export const reprocess = routeDefinition({
  path: '/api/admin/v1/reprocess',
  method: router.HTTPMethod.POST,
  authorize: authApi,
  validator: schema.createTsCodecValidator(internal_routes.ReprocessRequest, { allowAdditional: true }),
  handler: async (payload) => {
    const system = payload.context.system;

    const storage = system.storage;
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

    return internal_routes.ReprocessResponse.encode({
      connections: [
        {
          tag: system.config.connection!.tag,
          id: system.config.connection!.id,
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
    const system = payload.context.system;

    const content = payload.params.sync_rules;

    const schemaData = await api.getConnectionsSchema(system);
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

    const connectionStatus = await api.getConnectionStatus(system);
    if (connectionStatus == null) {
      return internal_routes.ValidateResponse.encode({
        errors: [{ level: 'fatal', message: 'No connection configured' }],
        connections: []
      });
    }

    const status = (await api.getSyncRulesStatus(sync_rules, system, {
      include_content: false,
      check_connection: connectionStatus?.connected,
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

export const ADMIN_ROUTES = [executeSql, diagnostics, getSchema, reprocess, validate];
