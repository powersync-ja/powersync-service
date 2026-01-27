import * as sdk from '@journeyapps-labs/common-sdk';
import { internal_routes } from '@powersync/service-types';

export class InstanceClient<C extends sdk.NetworkClient = sdk.NetworkClient> extends sdk.SDKClient<C> {
  executeSql = this.createEndpoint<internal_routes.ExecuteSqlRequest, internal_routes.ExecuteSqlResponse>({
    path: '/api/admin/v1/execute-sql'
  });

  diagnostics = this.createEndpoint<internal_routes.DiagnosticsRequest, internal_routes.DiagnosticsResponse>({
    path: '/api/admin/v1/diagnostics'
  });

  getSchema = this.createEndpoint<internal_routes.GetSchemaRequest, internal_routes.GetSchemaResponse>({
    path: '/api/admin/v1/schema'
  });

  validate = this.createEndpoint<internal_routes.ValidateRequest, internal_routes.ValidateResponse>({
    path: '/api/admin/v1/validate'
  });

  reprocess = this.createEndpoint<internal_routes.ReprocessRequest, internal_routes.ReprocessResponse>({
    path: '/api/admin/v1/reprocess'
  });
}
