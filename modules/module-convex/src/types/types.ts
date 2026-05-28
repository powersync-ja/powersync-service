import { ErrorCode, makeHostnameLookupFunction, ServiceError } from '@powersync/lib-services-framework';
import * as service_types from '@powersync/service-types';
import { LookupFunction } from 'node:net';
import * as t from 'ts-codec';

export const CONVEX_CONNECTION_TYPE = 'convex' as const;
const DEFAULT_POLLING_INTERVAL_MS = 1_000;
const DEFAULT_REQUEST_TIMEOUT_MS = 60_000;

export interface NormalizedConvexConnectionConfig {
  id: string;
  tag: string;

  deployment_url: string;
  deploy_key: string;

  debug_api: boolean;
  polling_interval_ms: number;
  request_timeout_ms: number;

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

  const pollingIntervalMs = options.polling_interval_ms ?? DEFAULT_POLLING_INTERVAL_MS;
  if (!Number.isFinite(pollingIntervalMs) || pollingIntervalMs <= 0) {
    throw new ServiceError(
      ErrorCode.PSYNC_S1109,
      `Convex connection: polling_interval_ms must be a positive finite number`
    );
  }

  const requestTimeoutMs = options.request_timeout_ms ?? DEFAULT_REQUEST_TIMEOUT_MS;
  if (!Number.isFinite(requestTimeoutMs) || requestTimeoutMs <= 0) {
    throw new ServiceError(
      ErrorCode.PSYNC_S1109,
      `Convex connection: request_timeout_ms must be a positive finite number`
    );
  }

  const lookup = makeHostnameLookupFunction(deploymentURL.hostname, {
    reject_ip_ranges: options.reject_ip_ranges ?? []
  });

  return {
    id: options.id ?? 'default',
    tag: options.tag ?? 'default',

    deployment_url: deploymentURL.toString().replace(/\/$/, ''),
    deploy_key: options.deploy_key,

    debug_api: options.debug_api ?? false,
    polling_interval_ms: pollingIntervalMs,
    request_timeout_ms: requestTimeoutMs,

    lookup
  };
}

export function resolveConvexConnectionConfig(config: ConvexConnectionConfig): ResolvedConvexConnectionConfig {
  return {
    ...config,
    ...normalizeConnectionConfig(config)
  };
}

export function baseUri(config: ResolvedConvexConnectionConfig) {
  return config.deployment_url;
}
