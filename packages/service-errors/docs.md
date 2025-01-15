# ErrorCode Documentation

## PSYNC_Rxxxx: Sync rules issues

- **PSYNC_R0001**
  Catch-all sync rules parsing error, if no more specific error is available

## PSYNC_R11xx: YAML syntax issues

## PSYNC_R12xx: YAML structure (schema) issues

## PSYNC_R21xx: SQL syntax issues

## PSYNC_R22xx: SQL supported feature issues

- **PSYNC_R2200**
  *(No doc comment)*

## PSYNC_R23xx: SQL schema mismatch issues

## PSYNC_R24xx: SQL security warnings

## PSYNC_Sxxxx: Service issues

- **PSYNC_S0001**
  Internal assertion.
  
  Examples:
  
    Migration ids must be strictly incrementing
    Metrics have not been initialized

- **PSYNC_S0102**
  TEARDOWN was not acknowledged.
  
  This happens when the TEARDOWN argument was not supplied when running
  the service teardown command.

## PSYNC_S1xxx: Replication issues

- **PSYNC_S1002**
  Row too large.
  
  There is a 15MB size limit on every replicated row - rows larger than
  this cannot be replicated.

- **PSYNC_S1003**
  Sync rules have been locked by another process for replication.

- **PSYNC_S1004**
  JSON nested object depth exceeds the limit of 20.

## PSYNC_S14xx: MongoDB storage replication issues

- **PSYNC_S1402**
  Max transaction tries exceeded.

## PSYNC_S11xx: Postgres replication issues

- **PSYNC_S1101**
  Replication assertion error.

- **PSYNC_S1103**
  Aborted initial replication.
  
  This is not an actual error - it is expected when the replication process
  is stopped, or if replication is stopped for any other reason.

- **PSYNC_S1104**
  cacert required for verify-ca

- **PSYNC_S1141**
  Publication does not exist. Run: `CREATE PUBLICATION powersync FOR ALL TABLES`

- **PSYNC_S1142**
  Publication does not publish all changes.

- **PSYNC_S1143**
  Publication uses publish_via_partition_root.

- **PSYNC_S1160**
  Replication slot does not exist anymore.

## PSYNC_S12xx: MySQL replication issues

## PSYNC_S13xx: MongoDB replication issues

- **PSYNC_S1341**
  Sharded MongoDB Clusters are not supported yet.

- **PSYNC_S1342**
  Standalone MongoDB instances are not supported - use a replicaset.

- **PSYNC_S1343**
  postImages not enabled

## PSYNC_S2xxx: Service API

- **PSYNC_S2001**
  Generic internal server error (HTTP 500).

- **PSYNC_S2002**
  Route not found (HTTP 404).

- **PSYNC_S2003**
  503 service unavailable due to restart.

## PSYNC_S21xx: Auth errors originating on the client.

## This does not include auth configuration errors on the service.

- **PSYNC_S2101**
  Generic auth error.

- **PSYNC_S2106**
  Parameters must be an object (`params` field in the JWT payload).

- **PSYNC_S2102**
  Token must expire in a maximum of ${maxAge} seconds
  Unexpected token algorithm ${header.alg}
  
  FIXME: duplicated code

- **PSYNC_S2103**
  Could not find an appropriate key in the keystore.
  The key is missing or no key matched the token KID

- **PSYNC_S2104**
  No token provided.

- **PSYNC_S2105**
  Unexpected "aud" claim value.

## PSYNC_S22xx: Auth integration errors

- **PSYNC_S2202**
  IPv6 not supported yet.

- **PSYNC_S2203**
  IPs in this range are not supported.

- **PSYNC_S2204**
  JWKS request failed.

## PSYNC_S23xx: Sync API errors

- **PSYNC_S2301**
  Internal assertion.
  
  Example: No context meta data provided

- **PSYNC_S2302**
  No sync rules available.

- **PSYNC_S2303**
  Timeout while waiting for checkpoint.

- **PSYNC_S2304**
  Maximum active concurrent connections limit has been reached.

- **PSYNC_S2311**
  Unable to deserialize frame.

## PSYNC_S23xx: Sync API errors - MongoDB Storage

- **PSYNC_S2401**
  Could not get clusterTime.

## PSYNC_S23xx: Sync API errors - Postgres Storage

## PSYNC_S3xxx: Service configuration issues

- **PSYNC_S3002**
  hostname required.

## PSYNC_S31xx: Auth configuration issues

- **PSYNC_S3102**
  Invalid jwks_uri.

- **PSYNC_S3103**
  Only http(s) is supported for jwks_uri.

- **PSYNC_S3200**
  Replication configuration issue.

- **PSYNC_S3201**
  Failed to validate module configuration.

- **PSYNC_S3202**
  database required

- **PSYNC_S3203**
  Explicit cacert is required for sslmode=verify-ca

## PSYNC_S4000: management / dev apis

- **PSYNC_S4001**
  Internal Assertion
  
  Examples:
  
   Unsupported query parameter
   Could not determine replication lag.

- **PSYNC_S4102**
  Auth disabled.

- **PSYNC_S4103**
  Authentication required.

- **PSYNC_S4104**
  No active sync rules.

- **PSYNC_S4105**
  Sync rules API disabled.


