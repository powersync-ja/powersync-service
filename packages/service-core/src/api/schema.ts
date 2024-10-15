import { internal_routes } from '@powersync/service-types';

import * as api from '../api/api-index.js';

export async function getConnectionsSchema(api: api.RouteAPI): Promise<internal_routes.GetSchemaResponse> {
  if (!api) {
    return {
      connections: [],
      defaultConnectionTag: 'default',
      defaultSchema: ''
    };
  }

  const baseConfig = await api.getSourceConfig();

  return {
    connections: [
      {
        schemas: await api.getConnectionSchema(),
        tag: baseConfig.tag!,
        id: baseConfig.id
      }
    ],
    defaultConnectionTag: baseConfig.tag!,
    defaultSchema: api.getParseSyncRulesOptions().defaultSchema
  };
}
