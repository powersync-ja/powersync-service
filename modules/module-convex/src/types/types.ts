import { ErrorCode, makeHostnameLookupFunction, ServiceError } from '@powersync/lib-services-framework';
import * as service_types from '@powersync/service-types';
import { LookupFunction } from 'node:net';
import * as t from 'ts-codec';

export const CONVEX_CONNECTION_TYPE = 'convex' as const;

export interface NormalizedConvexConnectionConfig {
  id: string;
  tag: string;

  deploymentUrl: string;
  deployKey: string;

  debugApi: boolean;
  pollingIntervalMs: number;
  requestTimeoutMs: number;

  lookup?: LookupFunction;
}

export const ConvexConnectionConfig = service_types.configFile.DataSourceConfig.and(
  t.object({
    type: t.literal(CONVEX_CONNECTION_TYPE),
    deployment_url: t.string,
    deploy_key: t.string,
    polling_interval_ms: t.number.optional(),
    request_timeout_ms: t.number.optional(),
    reject_ip_ranges: t.array(t.string).optional()
  })
);

export type ConvexConnectionConfig = t.Decoded<typeof ConvexConnectionConfig>;

export type ResolvedConvexConnectionConfig = ConvexConnectionConfig & NormalizedConvexConnectionConfig;

export function normalizeConnectionConfig(options: ConvexConnectionConfig): NormalizedConvexConnectionConfig {
  let deploymentURL: URL;
  try {
    deploymentURL = new URL(options.deployment_url);
  } catch (error) {
    throw new ServiceError(
      ErrorCode.PSYNC_S1109,
      `Convex connection: invalid deployment_url ${error instanceof Error ? `- ${error.message}` : ''}`
    );
  }

  if (deploymentURL.protocol != 'https:' && deploymentURL.protocol != 'http:') {
    throw new ServiceError(
      ErrorCode.PSYNC_S1109,
      `Convex connection: deployment_url must use http or https, got ${JSON.stringify(deploymentURL.protocol)}`
    );
  }

  if (deploymentURL.hostname == '') {
    throw new ServiceError(ErrorCode.PSYNC_S1106, `Convex connection: hostname required`);
  }

  if (options.deploy_key == '') {
    throw new ServiceError(ErrorCode.PSYNC_S1108, `Convex connection: deploy_key required`);
  }

  const lookup = makeHostnameLookupFunction(deploymentURL.hostname, {
    reject_ip_ranges: options.reject_ip_ranges ?? []
  });

  return {
    id: options.id ?? 'default',
    tag: options.tag ?? 'default',

    deploymentUrl: deploymentURL.toString().replace(/\/$/, ''),
    deployKey: options.deploy_key,

    debugApi: options.debug_api ?? false,
    pollingIntervalMs: options.polling_interval_ms ?? 1_000,
    requestTimeoutMs: options.request_timeout_ms ?? 30_000,

    lookup
  };
}

export function baseUri(config: ResolvedConvexConnectionConfig) {
  return config.deploymentUrl;
}
