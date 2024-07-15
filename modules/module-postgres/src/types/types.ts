import * as t from 'ts-codec';

import * as service_types from '@powersync/service-types';

export const PostgresConnectionConfig = service_types.configFile.dataSourceConfig.and(
  t.object({
    type: t.literal('postgresql'),
    /** Unique identifier for the connection - optional when a single connection is present. */
    id: t.string.optional(),
    /** Tag used as reference in sync rules. Defaults to "default". Does not have to be unique. */
    tag: t.string.optional(),
    uri: t.string.optional(),
    hostname: t.string.optional(),
    port: service_types.configFile.portCodec.optional(),
    username: t.string.optional(),
    password: t.string.optional(),
    database: t.string.optional(),

    /** Defaults to verify-full */
    sslmode: t.literal('verify-full').or(t.literal('verify-ca')).or(t.literal('disable')).optional(),
    /** Required for verify-ca, optional for verify-full */
    cacert: t.string.optional(),

    client_certificate: t.string.optional(),
    client_private_key: t.string.optional(),

    /** Expose database credentials */
    demo_database: t.boolean.optional(),
    /** Expose "execute-sql" */
    debug_api: t.boolean.optional(),

    /**
     * Prefix for the slot name. Defaults to "powersync_"
     */
    slot_name_prefix: t.string.optional()
  })
);

export type PostgresConnectionConfig = t.Decoded<typeof PostgresConnectionConfig>;
