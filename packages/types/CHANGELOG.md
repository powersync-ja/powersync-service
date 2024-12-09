# @powersync/service-types

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
