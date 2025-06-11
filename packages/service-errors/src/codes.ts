/**
 * Error codes used across the service.
 *
 * This is the primary definition of error codes, as well as the documentation
 * for each.
 */
export enum ErrorCode {
  // # PSYNC_Rxxxx: Sync rules issues

  /**
   * Catch-all sync rules parsing error, if no more specific error is available
   */
  PSYNC_R0001 = 'PSYNC_R0001',

  // ## PSYNC_R11xx: YAML syntax issues

  // ## PSYNC_R12xx: YAML structure (schema) issues

  // ## PSYNC_R21xx: SQL syntax issues

  // ## PSYNC_R22xx: SQL supported feature issues

  // ## PSYNC_R23xx: SQL schema mismatch issues

  // ## PSYNC_R24xx: SQL security warnings

  // # PSYNC_Sxxxx: Service issues

  /**
   * Internal assertion.
   *
   * If you see this error, it might indicate a bug in the service code.
   */
  PSYNC_S0001 = 'PSYNC_S0001',

  /**
   * TEARDOWN was not acknowledged.
   *
   * This happens when the TEARDOWN argument was not supplied when running
   * the service teardown command. The TEARDOWN argument is required since
   * this is a destructive command.
   *
   * Run the command with `teardown TEARDOWN` to confirm.
   */
  PSYNC_S0102 = 'PSYNC_S0102',

  // ## PSYNC_S1xxx: Replication issues

  /**
   * Row too large.
   *
   * There is a 15MB size limit on every replicated row - rows larger than
   * this cannot be replicated.
   */
  PSYNC_S1002 = 'PSYNC_S1002',

  /**
   * Sync rules have been locked by another process for replication.
   *
   * This error is normal in some circumstances:
   * 1. In some cases, if a process was forcefully terminated, this error may occur for up to a minute.
   * 2. During rolling deploys, this error may occur until the old process stops replication.
   *
   * If the error persists for longer, this may indicate that multiple replication processes are running.
   * Make sure there is only one replication process apart from rolling deploys.
   */
  PSYNC_S1003 = 'PSYNC_S1003',

  /**
   * JSON nested object depth exceeds the limit of 20.
   *
   * This may occur if there is very deep nesting in JSON or embedded documents.
   */
  PSYNC_S1004 = 'PSYNC_S1004',

  // ## PSYNC_S11xx: Postgres replication issues

  /**
   * Replication assertion error.
   *
   * If you see this error, it might indicate a bug in the service code.
   */
  PSYNC_S1101 = 'PSYNC_S1101',

  /**
   * Aborted initial replication.
   *
   * This is not an actual error - it is expected when the replication process
   * is stopped, or if replication is stopped for any other reason.
   */
  PSYNC_S1103 = 'PSYNC_S1103',

  /**
   * Explicit cacert is required for `sslmode: verify-ca`.
   *
   * Use either verify-full, or specify a certificate with verify-ca.
   */
  PSYNC_S1104 = 'PSYNC_S1104',

  /**
   * `database` is required in connection config.
   *
   * Specify the database explicitly, or in the `uri` field.
   */
  PSYNC_S1105 = 'PSYNC_S1105',

  /**
   * `hostname` is required in connection config.
   *
   * Specify the hostname explicitly, or in the `uri` field.
   */
  PSYNC_S1106 = 'PSYNC_S1106',

  /**
   * `username` is required in connection config.
   *
   * Specify the username explicitly, or in the `uri` field.
   */
  PSYNC_S1107 = 'PSYNC_S1107',

  /**
   * `password` is required in connection config.
   *
   * Specify the password explicitly, or in the `uri` field.
   */
  PSYNC_S1108 = 'PSYNC_S1108',

  /**
   * Invalid database URI.
   *
   * Check the URI scheme and format.
   */
  PSYNC_S1109 = 'PSYNC_S1109',

  /**
   * Invalid port number.
   *
   * Only ports in the range 1024 - 65535 are supported.
   */
  PSYNC_S1110 = 'PSYNC_S1110',

  /**
   * Publication does not exist.
   *
   * Run: `CREATE PUBLICATION powersync FOR ALL TABLES` on the source database.
   */
  PSYNC_S1141 = 'PSYNC_S1141',

  /**
   * Publication does not publish all changes.
   *
   * Create a publication using `WITH (publish = "insert, update, delete, truncate")` (the default).
   */
  PSYNC_S1142 = 'PSYNC_S1142',

  /**
   * Publication uses publish_via_partition_root.
   */
  PSYNC_S1143 = 'PSYNC_S1143',

  /**
   * Invalid Postgres server configuration for replication and sync bucket storage.
   *
   * The same Postgres server, running an unsupported version of Postgres, has been configured for both replication and sync bucket storage.
   * Using the same Postgres server is only supported on Postgres 14 and above.
   * This error typically indicates that the Postgres version is below 14.
   * Either upgrade the Postgres server to version 14 or above, or use a different Postgres server for sync bucket storage.
   */
  PSYNC_S1144 = 'PSYNC_S1144',

  /**
   * Table has RLS enabled, but the replication role does not have the BYPASSRLS attribute.
   *
   * We recommend using a dedicated replication role with the BYPASSRLS attribute for replication:
   *
   *     ALTER ROLE powersync_role BYPASSRLS
   *
   * An alternative is to create explicit policies for the replication role. If you have done that,
   * you may ignore this warning.
   */
  PSYNC_S1145 = 'PSYNC_S1145',

  // ## PSYNC_S12xx: MySQL replication issues

  // ## PSYNC_S13xx: MongoDB replication issues

  /**
   * Generic MongoServerError.
   */
  PSYNC_S1301 = 'PSYNC_S1301',

  /**
   * Generic MongoNetworkError.
   */
  PSYNC_S1302 = 'PSYNC_S1302',

  /**
   * MongoDB internal TLS error.
   *
   * If connection to a shared cluster on MongoDB Atlas, this could be an IP Access List issue.
   * Check that the service IP is allowed to connect to the cluster.
   */
  PSYNC_S1303 = 'PSYNC_S1303',

  /**
   * MongoDB connection DNS error.
   *
   * Check that the hostname is correct.
   */
  PSYNC_S1304 = 'PSYNC_S1304',

  /**
   * MongoDB connection timeout.
   *
   * Check that the hostname is correct, and that the service IP is allowed to connect to the cluster.
   */
  PSYNC_S1305 = 'PSYNC_S1305',

  /**
   * MongoDB authentication error.
   *
   * Check the username and password.
   */
  PSYNC_S1306 = 'PSYNC_S1306',

  /**
   * MongoDB authorization error.
   *
   * Check that the user has the required privileges.
   */
  PSYNC_S1307 = 'PSYNC_S1307',

  /**
   * Sharded MongoDB Clusters are not supported yet.
   */
  PSYNC_S1341 = 'PSYNC_S1341',

  /**
   * Standalone MongoDB instances are not supported - use a replica-set.
   */
  PSYNC_S1342 = 'PSYNC_S1342',

  /**
   * PostImages not enabled on a source collection.
   *
   * Use `post_images: auto_configure` to configure post images automatically, or enable manually:
   *
   *     db.runCommand({
   *       collMod: 'collection-name',
   *       changeStreamPreAndPostImages: { enabled: true }
   *     });
   */
  PSYNC_S1343 = 'PSYNC_S1343',

  /**
   * The MongoDB Change Stream has been invalidated.
   *
   * Possible causes:
   * - Some change stream documents do not have postImages.
   * - startAfter/resumeToken is not valid anymore.
   * - The replication connection has changed.
   * - The database has been dropped.
   *
   * Replication will be stopped for this Change Stream. Replication will restart with a new Change Stream.
   */
  PSYNC_S1344 = 'PSYNC_S1344',

  /**
   * Failed to read MongoDB Change Stream due to a timeout.
   *
   * This may happen if there is a significant delay on the source database in reading the change stream.
   *
   * If this is not resolved after retries, replication may need to be restarted from scratch.
   */
  PSYNC_S1345 = 'PSYNC_S1345',

  /**
   * Failed to read MongoDB Change Stream.
   *
   * See the error cause for more details.
   */
  PSYNC_S1346 = 'PSYNC_S1346',

  // ## PSYNC_S14xx: MongoDB storage replication issues

  /**
   * Max transaction tries exceeded.
   */
  PSYNC_S1402 = 'PSYNC_S1402',

  // ## PSYNC_S2xxx: Service API

  /**
   * Generic internal server error (HTTP 500).
   *
   * See the error details for more info.
   */
  PSYNC_S2001 = 'PSYNC_S2001',

  /**
   * Route not found (HTTP 404).
   */
  PSYNC_S2002 = 'PSYNC_S2002',

  /**
   * 503 service unavailable due to restart.
   *
   * Wait a while then retry the request.
   */
  PSYNC_S2003 = 'PSYNC_S2003',

  /**
   * 415 unsupported media type.
   *
   * This code always indicates an issue with the client.
   */
  PSYNC_S2004 = 'PSYNC_S2004',

  // ## PSYNC_S21xx: Auth errors originating on the client.
  //
  // This does not include auth configuration errors on the service.

  /**
   * Generic authentication error.
   */
  PSYNC_S2101 = 'PSYNC_S2101',

  /**
   * Could not verify the auth token signature.
   *
   * Typical causes include:
   * 1. Token kid is not found in the keystore.
   * 2. Signature does not match the kid in the keystore.
   */
  PSYNC_S2102 = 'PSYNC_S2102',

  /**
   * Token has expired. Check the expiry date on the token.
   */
  PSYNC_S2103 = 'PSYNC_S2103',

  /**
   * Token expiration period is too long. Issue shorter-lived tokens.
   */
  PSYNC_S2104 = 'PSYNC_S2104',

  /**
   * Token audience does not match expected values.
   *
   * Check the aud value on the token, compared to the audience values allowed in the service config.
   */
  PSYNC_S2105 = 'PSYNC_S2105',

  /**
   * No token provided. An auth token is required for every request.
   *
   * The Authorization header must start with "Token" or "Bearer", followed by the JWT.
   */
  PSYNC_S2106 = 'PSYNC_S2106',

  // ## PSYNC_S22xx: Auth integration errors

  /**
   * Generic auth configuration error. See the message for details.
   */
  PSYNC_S2201 = 'PSYNC_S2201',

  /**
   * IPv6 support is not enabled for the JWKS URI.
   *
   * Use an endpoint that supports IPv4.
   */
  PSYNC_S2202 = 'PSYNC_S2202',

  /**
   * IPs in this range are not supported.
   *
   * Make sure to use a publicly-accessible JWKS URI.
   */
  PSYNC_S2203 = 'PSYNC_S2203',

  /**
   * JWKS request failed.
   */
  PSYNC_S2204 = 'PSYNC_S2204',

  // ## PSYNC_S23xx: Sync API errors

  /**
   * No sync rules available.
   *
   * This error may happen if:
   * 1. Sync rules have not been deployed.
   * 2. Sync rules have been deployed, but is still busy processing.
   *
   * View the replicator logs to see if the sync rules are being processed.
   */
  PSYNC_S2302 = 'PSYNC_S2302',

  /**
   * Maximum active concurrent connections limit has been reached.
   */
  PSYNC_S2304 = 'PSYNC_S2304',

  /**
   * Too many buckets.
   *
   * There is currently a limit of 1000 buckets per active connection.
   */
  PSYNC_S2305 = 'PSYNC_S2305',

  // ## PSYNC_S23xx: Sync API errors - MongoDB Storage

  /**
   * Could not get clusterTime.
   */
  PSYNC_S2401 = 'PSYNC_S2401',

  // ## PSYNC_S23xx: Sync API errors - Postgres Storage

  // ## PSYNC_S3xxx: Service configuration issues

  // ## PSYNC_S31xx: Auth configuration issues

  /**
   * Invalid jwks_uri.
   */
  PSYNC_S3102 = 'PSYNC_S3102',

  /**
   * Only http(s) is supported for jwks_uri.
   */
  PSYNC_S3103 = 'PSYNC_S3103',

  // ## PSYNC_S32xx: Replication configuration issue.

  /**
   * Failed to validate module configuration.
   */
  PSYNC_S3201 = 'PSYNC_S3201',

  // ## PSYNC_S4000: Management / Dev APIs

  /**
   * Internal assertion error.
   *
   * This error may indicate a bug in the service code.
   */
  PSYNC_S4001 = 'PSYNC_S4001',

  /**
   * No active sync rules.
   */
  PSYNC_S4104 = 'PSYNC_S4104',

  /**
   * Sync rules API disabled.
   *
   * When a sync rules file is configured, the dynamic sync rules API is disabled.
   */
  PSYNC_S4105 = 'PSYNC_S4105'
}
