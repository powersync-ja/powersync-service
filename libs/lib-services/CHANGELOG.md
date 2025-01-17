# @powersync/lib-services-framework

## 0.4.0

### Minor Changes

- 9d9ff08: Improved migrations logic. Up migrations can be executed correctly after down migrations.

## 0.3.0

### Minor Changes

- fea550f: Made migrations more pluggable

### Patch Changes

- fea550f: Updated ts-codec to 1.3.0 for better decode error responses

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
