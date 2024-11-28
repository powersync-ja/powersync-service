# @powersync/service-jpgwire

## 0.18.1

### Patch Changes

- Updated dependencies [35c267f]
  - @powersync/service-types@0.4.0

## 0.18.0

### Minor Changes

- 57bd18b: - Introduced modules to the powersync service architecture
  - Core functionality has been moved to "engine" classes. Modules can register additional functionality with these engines.
  - The sync API functionality used by the routes has been abstracted to an interface. API routes are now managed by the RouterEngine.
  - Replication is managed by the ReplicationEngine and new replication data sources can be registered to the engine by modules.
  - Refactored existing Postgres replication as a module.
  - Removed Postgres specific code from the core service packages.

### Patch Changes

- Updated dependencies [57bd18b]
- Updated dependencies [57bd18b]
- Updated dependencies [57bd18b]
- Updated dependencies [57bd18b]
- Updated dependencies [57bd18b]
- Updated dependencies [57bd18b]
  - @powersync/service-sync-rules@0.21.0
  - @powersync/service-types@0.3.0

## 0.17.14

### Patch Changes

- Updated dependencies [c9ad713]
  - @powersync/service-types@0.2.0

## 0.17.13

### Patch Changes

- 7587a74: Fix date parsing in replication for dates further back than 100 AD.

## 0.17.12

### Patch Changes

- 5f5163f: Fix performance issue when reading a lot of data from a socket.

## 0.17.11

### Patch Changes

- Updated dependencies [3d9feb2]
  - @powersync/service-types@0.1.0

## 0.17.10

### Patch Changes

- 285f368: Initial public release
- Updated dependencies [285f368]
  - @powersync/service-jsonbig@0.17.10
  - @powersync/service-types@0.0.2
