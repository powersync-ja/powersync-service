import { container } from '@powersync/lib-services-framework';
import { internal_routes } from '@powersync/service-types';

import { ServiceContext } from '../system/ServiceContext.js';

export async function getConnectionsSchema(): Promise<internal_routes.GetSchemaResponse> {
  const { syncAPIProvider } = container.getImplementation(ServiceContext);

  const api = syncAPIProvider.getSyncAPI();
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
