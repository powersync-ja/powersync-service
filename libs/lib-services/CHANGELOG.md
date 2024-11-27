# @powersync/lib-services-framework

## 0.2.0

### Minor Changes

- 57bd18b: Added disposable listeners and observers
- 57bd18b: - Introduced modules to the powersync service architecture
  - Core functionality has been moved to "engine" classes. Modules can register additional functionality with these engines.
  - The sync API functionality used by the routes has been abstracted to an interface. API routes are now managed by the RouterEngine.
  - Replication is managed by the ReplicationEngine and new replication data sources can be registered to the engine by modules.
  - Refactored existing Postgres replication as a module.
  - Removed Postgres specific code from the core service packages.

## 0.1.1

### Patch Changes

- 909f71a: Fix concurrent connection limiting for websockets

## 0.1.0

### Minor Changes

- cbf2683: Initial release for service utillities
