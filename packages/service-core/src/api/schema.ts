import { internal_routes } from '@powersync/service-types';

import * as api from '../api/api-index.js';

export async function getConnectionsSchema(api: api.RouteAPI | null): Promise<internal_routes.GetSchemaResponse> {
  if (!api) {
    return {
      connections: []
    };
  }

  const baseConfig = await api.getSourceConfig();
  const schemas = await api.getConnectionSchema();
  /**
   * This function contains references to `pg_type`
   * for columns. The API method is agnostic to Postgres.
   * Some mapping is done to maintain compatibility.
   *  */
  return {
    connections: [
      {
        id: baseConfig.id,
        tag: baseConfig.tag,
        schemas: schemas.map((schema) => ({
          ...schema,
          tables: schema.tables.map((table) => ({
            ...table,
            columns: table.columns.map((column) => ({
              ...column,
              // Defaults back to the original type if not provided
              pg_type: column.internal_type ?? column.type
            }))
          }))
        }))
      }
    ]
  };
}
