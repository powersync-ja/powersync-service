import { internal_routes } from '@powersync/service-types';

import * as system from '../system/system-index.js';

export async function getConnectionsSchema(
  serviceContext: system.ServiceContext
): Promise<internal_routes.GetSchemaResponse> {
  const api = serviceContext.routerEngine.getAPI();
  if (!api) {
    return {
      connections: []
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
    ]
  };
}
