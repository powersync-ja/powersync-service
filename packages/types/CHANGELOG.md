# @powersync/service-types

## 0.13.0

### Minor Changes

- 86807d0: Support gzip and zstd compression in http streams.

### Patch Changes

- f56acce: Enable permessage-deflate for websockets.
- 060b829: Update license abbreviation to FSL-1.1-ALv2.

## 0.12.1

### Patch Changes

- ba1ceef: Remove unused dev config.

## 0.12.0

### Minor Changes

- 0ccd470: Add powersync_replication_lag_seconds metric

## 0.11.0

### Minor Changes

- c672380: Added JSON schema export for base PowerSyncConfig
- ca0a566: Added healthcheck types to PowerSyncConfig

## 0.10.0

### Minor Changes

- d1b83ce: Refactored Metrics to use a MetricsEngine which is telemetry framework agnostic.

## 0.9.0

### Minor Changes

- ba7baeb: Make some service limits configurable.

## 0.8.0

### Minor Changes

- 4b43cdb: Exit replication process when sync rules are not valid; configurable with a new `sync_rules.exit_on_error` option.

## 0.7.1

### Patch Changes

- 8675236: Minor config changes.

## 0.7.0

### Minor Changes

- fea550f: Moved MongoDB sync bucket storage implementation to the MongoDB module.

## 0.6.0

### Minor Changes

- 0bf1309: Add ECDSA support for JWTs

## 0.5.0

### Minor Changes

- ebc62ff: Add EdDSA support for JWTs.

## 0.4.0

### Minor Changes

- 35c267f: Add "supabase_jwt_secret" config option to simplify static Supabase auth.

## 0.3.0

### Minor Changes

- 57bd18b: - Introduced modules to the powersync service architecture
  - Core functionality has been moved to "engine" classes. Modules can register additional functionality with these engines.
  - The sync API functionality used by the routes has been abstracted to an interface. API routes are now managed by the RouterEngine.
  - Replication is managed by the ReplicationEngine and new replication data sources can be registered to the engine by modules.
  - Refactored existing Postgres replication as a module.
  - Removed Postgres specific code from the core service packages.

### Patch Changes

- 57bd18b: Updates from Replication events changes

## 0.2.0

### Minor Changes

- c9ad713: Removed unused development routes

## 0.1.0

### Minor Changes

- 3d9feb2: Added the ability to capture anonymous usage metrics

## 0.0.2

### Patch Changes

- 285f368: Initial public release
