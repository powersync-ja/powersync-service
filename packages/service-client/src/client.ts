import * as sdk from '@journeyapps-labs/common-sdk';
import { internal_routes } from '@powersync/service-types';

/**
 * Client for interacting with a PowerSync instance's admin API.
 *
 * @note The Authorization token should be taken from the PowerSync config YAML section:
 * ```yaml
 * api:
 *   tokens:
 *     - use_a_better_token_in_production
 * ```
 *
 * Example usage:
 *
 * ```typescript
 * import * as sdk from '@journeyapps-labs/common-sdk';
 * import { InstanceClient } from '@powersync/service-client';
 *
 * const client = new InstanceClient({
 *   client: sdk.createNodeNetworkClient({
 *     headers: () => ({
 *       'Authorization': 'Bearer <token-from-config>'
 *     })
 *   }),
 *   endpoint: 'https://[Your PowerSync URL here]'
 * });
 * ```
 */
export class InstanceClient<C extends sdk.NetworkClient = sdk.NetworkClient> extends sdk.SDKClient<C> {
  /**
   * Get diagnostics and health information from the instance.
   * Optionally include sync config content in the response.
   *
   * Example:
   * ```typescript
   * const diag = await client.diagnostics({ sync_rules_content: true });
   * // {
   * //   connections: [
   * //     {
   * //       id: 'default',
   * //       postgres_uri: 'postgres://user@host/db',
   * //       connected: true,
   * //       errors: []
   * //     }
   * //   ],
   * //   active_sync_rules: {
   * //     content: 'bucket "default" { ... }',
   * //     connections: [
   * //       {
   * //         id: 'default',
   * //         tag: 'default',
   * //         slot_name: 'powersync_default',
   * //         initial_replication_done: true,
   * //         last_lsn: '0/16B6C50',
   * //         tables: [
   * //           {
   * //             schema: 'public',
   * //             name: 'users',
   * //             replication_id: ['id'],
   * //             data_queries: true,
   * //             parameter_queries: false,
   * //             errors: []
   * //           }
   * //         ]
   * //       }
   * //     ],
   * //     errors: []
   * //   },
   * //   deploying_sync_rules: undefined
   * // }
   * ```
   */
  diagnostics = this.createEndpoint<internal_routes.DiagnosticsRequest, internal_routes.DiagnosticsResponse>({
    path: '/api/admin/v1/diagnostics'
  });

  /**
   * Retrieve the current schema from the instance.
   *
   * Example:
   * ```typescript
   * const schema = await client.getSchema({});
   * // {
   * //   connections: [
   * //     {
   * //       id: 'default',
   * //       tag: 'default',
   * //       schemas: [
   * //         {
   * //           name: 'public',
   * //           tables: [
   * //             {
   * //               name: 'users',
   * //               columns: [
   * //                 { name: 'id', sqlite_type: 'integer', internal_type: 'int8', type: 'bigint', pg_type: 'int8' },
   * //                 { name: 'email', sqlite_type: 'text', internal_type: 'varchar', type: 'character varying', pg_type: 'varchar' }
   * //               ]
   * //             }
   * //           ]
   * //         }
   * //       ]
   * //     }
   * //   ],
   * //   defaultConnectionTag: 'default',
   * //   defaultSchema: 'public'
   * // }
   * ```
   */
  getSchema = this.createEndpoint<internal_routes.GetSchemaRequest, internal_routes.GetSchemaResponse>({
    path: '/api/admin/v1/schema'
  });

  /**
   * Validate the current configuration and schema.
   *
   * Example:
   * ```typescript
   * const validation = await client.validate({
   *   sync_rules: `bucket_definitions:
   *   documents:
   *     priority: 0
   *     parameters: SELECT (request.parameters() ->> 'document_id') as document_id
   *     data:
   *       - SELECT * FROM documents WHERE id = bucket.document_id
   * `
   * });
   * // {
   * //   content: 'bucket_definitions: ...',
   * //   connections: [
   * //     {
   * //       id: 'default',
   * //       tag: 'default',
   * //       slot_name: 'powersync_default',
   * //       initial_replication_done: true,
   * //       last_lsn: '0/16B6C50',
   * //       tables: [
   * //         {
   * //           schema: 'public',
   * //           name: 'documents',
   * //           replication_id: ['id'],
   * //           data_queries: true,
   * //           parameter_queries: false,
   * //           errors: []
   * //         }
   * //       ]
   * //     }
   * //   ],
   * //   errors: []
   * // }
   * ```
   */
  validate = this.createEndpoint<internal_routes.ValidateRequest, internal_routes.ValidateResponse>({
    path: '/api/admin/v1/validate'
  });

  /**
   * Reprocess sync state for the instance.
   *
   * Example:
   * ```typescript
   * const resp = await client.reprocess({});
   * // {
   * //   connections: [
   * //     {
   * //       id: 'default',
   * //       tag: 'default',
   * //       slot_name: 'powersync_default'
   * //     }
   * //   ]
   * // }
   * ```
   */
  reprocess = this.createEndpoint<internal_routes.ReprocessRequest, internal_routes.ReprocessResponse>({
    path: '/api/admin/v1/reprocess'
  });

  /**
   * Per-bucket storage report for the active sync config: exact operation counts, with rows and
   * fragmentation derived from each bucket's last full compact (null until a bucket has been fully
   * compacted, and on storage versions without compact statistics).
   *
   * Example:
   * ```typescript
   * const report = await client.bucketReport({ limit: 50 });
   * // {
   * //   buckets: [
   * //     {
   * //       bucket: 'by_user["u1"]',
   * //       operations: 502,
   * //       operation_bytes: 54149,
   * //       uncompacted_operations: 0,
   * //       rows: 3,
   * //       fragmentation: 167.3,
   * //       last_full_compact_at: '2026-08-21T11:26:19.689Z',
   * //       next_compact_at: null,
   * //       suggested_action: 'defragment'
   * //     }
   * //   ],
   * //   definitions: [
   * //     { definition: 'by_user', bucket_count: 4, operations: 72, ... }
   * //   ],
   * //   totals: { bucket_count: 10, operations: 50, operation_bytes: 15031, estimated: false },
   * //   buckets_truncated: false,
   * //   definitions_truncated: false
   * // }
   * ```
   */
  bucketReport = this.createEndpoint<internal_routes.BucketReportRequest, internal_routes.BucketReportResponse>({
    path: '/api/admin/v1/bucket-report'
  });
}
