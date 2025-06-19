# @powersync/lib-services-framework

## 0.7.0

### Minor Changes

- 951b010: Implement resuming of initial replication snapshots.

### Patch Changes

- f9e8673: [MongoDB Storage] Handle connection errors on startup
- Updated dependencies [951b010]
- Updated dependencies [f9e8673]
  - @powersync/service-errors@0.3.1

## 0.6.0

### Minor Changes

- 9dc4e01: Improve authentication error messages and logs
- 94f657d: Add additional log metadata to sync requests.
- ca0a566: Switched default health check probe mechanism from filesystem to in-memory implementation. Consumers now need to manually opt-in to filesystem probes.

### Patch Changes

- d154682: [MongoDB] Add support for plain "mongodb://" URIs for replica sets (multiple hostnames).
- Updated dependencies [9dc4e01]
- Updated dependencies [d869876]
  - @powersync/service-errors@0.3.0

## 0.5.4

### Patch Changes

- Updated dependencies [08f6ae8]
  - @powersync/service-errors@0.2.2

## 0.5.3

### Patch Changes

- b4fe4ae: Upgrade mongodb and bson packages, removing the need for some workarounds.

## 0.5.2

### Patch Changes

- Updated dependencies [436eee6]
  - @powersync/service-errors@0.2.1

## 0.5.1

### Patch Changes

- Updated dependencies [d053e84]
  - @powersync/service-errors@0.2.0

## 0.5.0

### Minor Changes

- 8675236: Allow limiting IP ranges of outgoing connections

### Patch Changes

- f049aa9: Introduce standard error codes
- Updated dependencies [f049aa9]
  - @powersync/service-errors@0.1.1

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
