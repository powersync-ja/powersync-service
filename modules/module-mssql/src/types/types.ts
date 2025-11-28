import { ErrorCode, makeHostnameLookupFunction, ServiceError } from '@powersync/lib-services-framework';
import * as service_types from '@powersync/service-types';
import { LookupFunction } from 'node:net';
import * as t from 'ts-codec';
import * as urijs from 'uri-js';

export const MSSQL_CONNECTION_TYPE = 'mssql' as const;

export const AzureActiveDirectoryPasswordAuthentication = t.object({
  type: t.literal('azure-active-directory-password'),
  options: t.object({
    /**
     * A user need to provide `userName` associate to their account.
     */
    userName: t.string,
    /**
     * A user need to provide `password` associate to their account.
     */
    password: t.string,
    /**
     * A client id to use.
     */
    clientId: t.string,
    /**
     * Azure tenant ID
     */
    tenantId: t.string
  })
});
export type AzureActiveDirectoryPasswordAuthentication = t.Decoded<typeof AzureActiveDirectoryPasswordAuthentication>;

export const AzureActiveDirectoryServicePrincipalSecret = t.object({
  type: t.literal('azure-active-directory-service-principal-secret'),
  options: t.object({
    /**
     * Application (`client`) ID from your registered Azure application
     */
    clientId: t.string,
    /**
     * The created `client secret` for this registered Azure application
     */
    clientSecret: t.string,
    /**
     * Directory (`tenant`) ID from your registered Azure application
     */
    tenantId: t.string
  })
});
export type AzureActiveDirectoryServicePrincipalSecret = t.Decoded<typeof AzureActiveDirectoryServicePrincipalSecret>;

export const DefaultAuthentication = t.object({
  type: t.literal('default'),
  options: t.object({
    /**
     * User name to use for sql server login.
     */
    userName: t.string,
    /**
     * Password to use for sql server login.
     */
    password: t.string
  })
});
export type DefaultAuthentication = t.Decoded<typeof DefaultAuthentication>;

export const AdditionalConfig = t.object({
  /**
   * Interval in milliseconds to wait between polling cycles. Defaults to 1000 milliseconds.
   */
  pollingIntervalMs: t.number.optional(),
  /**
   * Maximum number of transactions to poll per polling cycle. Defaults to 10.
   */
  pollingBatchSize: t.number.optional(),

  /**
   *  Whether to trust the server certificate. Set to true for local development and self-signed certificates.
   *  Default is false.
   */
  trustServerCertificate: t.boolean.optional()
});

export interface AdditionalConfig {
    /**
   * Interval in milliseconds to wait between polling cycles. Defaults to 1000 milliseconds.
   */
  pollingIntervalMs: number;
  /**
   * Maximum number of transactions to poll per polling cycle. Defaults to 10.
   */
  pollingBatchSize: number;
  /**
   *  Whether to trust the server certificate. Set to true for local development and self-signed certificates.
   *  Default is false.
   */
  trustServerCertificate: boolean;
}

export type AuthenticationType =
  | DefaultAuthentication
  | AzureActiveDirectoryPasswordAuthentication
  | AzureActiveDirectoryServicePrincipalSecret;

export interface NormalizedMSSQLConnectionConfig {
  id: string;
  tag: string;

  username?: string;
  password?: string;
  hostname: string;
  port: number;
  database: string;
  schema?: string;

  authentication?: AuthenticationType;

  lookup?: LookupFunction;

  additionalConfig: AdditionalConfig;
}

export const MSSQLConnectionConfig = service_types.configFile.DataSourceConfig.and(
  t.object({
    type: t.literal(MSSQL_CONNECTION_TYPE),
    uri: t.string.optional(),
    username: t.string.optional(),
    password: t.string.optional(),
    database: t.string.optional(),
    schema: t.string.optional(),
    hostname: t.string.optional(),
    port: service_types.configFile.portCodec.optional(),

    authentication: DefaultAuthentication.or(AzureActiveDirectoryPasswordAuthentication)
      .or(AzureActiveDirectoryServicePrincipalSecret)
      .optional(),

    reject_ip_ranges: t.array(t.string).optional(),
    additionalConfig: AdditionalConfig.optional()
  })
);

/**
 * Config input specified when starting services
 */
export type MSSQLConnectionConfig = t.Decoded<typeof MSSQLConnectionConfig>;

/**
 * Resolved version of {@link MSSQLConnectionConfig}
 */
export type ResolvedMSSQLConnectionConfig = MSSQLConnectionConfig & NormalizedMSSQLConnectionConfig;

/**
 * Validate and normalize connection options.
 *
 * Returns destructured options.
 */
export function normalizeConnectionConfig(options: MSSQLConnectionConfig): NormalizedMSSQLConnectionConfig {
  let uri: urijs.URIComponents;
  if (options.uri) {
    uri = urijs.parse(options.uri);
    if (uri.scheme != 'mssql') {
      throw new ServiceError(
        ErrorCode.PSYNC_S1109,
        `Invalid URI - protocol must be mssql, got ${JSON.stringify(uri.scheme)}`
      );
    }
  } else {
    uri = urijs.parse('mssql:///');
  }

  const hostname = options.hostname ?? uri.host ?? '';
  const port = Number(options.port ?? uri.port ?? 1433);

  const database = options.database ?? uri.path?.substring(1) ?? '';

  const [uri_username, uri_password] = (uri.userinfo ?? '').split(':');

  const username = options.username ?? uri_username ?? '';
  const password = options.password ?? uri_password ?? '';

  if (hostname == '') {
    throw new ServiceError(ErrorCode.PSYNC_S1106, `MSSQL connection: hostname required`);
  }

  if (username == '' && !options.authentication) {
    throw new ServiceError(ErrorCode.PSYNC_S1107, `MSSQL connection: username or authentication config is required`);
  }

  if (password == '' && !options.authentication) {
    throw new ServiceError(ErrorCode.PSYNC_S1108, `MSSQL connection: password or authentication config is required`);
  }

  if (database == '') {
    throw new ServiceError(ErrorCode.PSYNC_S1105, `MSSQL connection: database required`);
  }

  const lookup = makeHostnameLookupFunction(hostname, { reject_ip_ranges: options.reject_ip_ranges ?? [] });

  return {
    id: options.id ?? 'default',
    tag: options.tag ?? 'default',

    username,
    password,
    hostname,
    port,
    database,

    lookup,
    authentication: options.authentication,

    additionalConfig: {
      pollingIntervalMs: options.additionalConfig?.pollingIntervalMs ?? 1000,
      pollingBatchSize: options.additionalConfig?.pollingBatchSize ?? 10,
      trustServerCertificate: options.additionalConfig?.trustServerCertificate ?? false
    }
  } satisfies NormalizedMSSQLConnectionConfig;
}

export function baseUri(config: ResolvedMSSQLConnectionConfig) {
  return `mssql://${config.hostname}:${config.port}/${config.database}`;
}
