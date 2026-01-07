# @powersync/lib-services-framework

## 0.7.14

### Patch Changes

- Updated dependencies [bdfd287]
  - @powersync/service-sync-rules@0.29.10

## 0.7.13

### Patch Changes

- Updated dependencies [21b3a41]
  - @powersync/service-sync-rules@0.29.9

## 0.7.12

### Patch Changes

- Updated dependencies [b77bb2c]
  - @powersync/service-errors@0.3.6

## 0.7.11

### Patch Changes

- Updated dependencies [0156d10]
  - @powersync/service-sync-rules@0.29.8

## 0.7.10

### Patch Changes

- Updated dependencies [fff0024]
  - @powersync/service-sync-rules@0.29.7

## 0.7.9

### Patch Changes

- 80fd68b: Add SDK usage reporting support.
- 88982d9: Migrate to trusted publishing
- Updated dependencies [b4fa979]
- Updated dependencies [c2bd0b0]
- Updated dependencies [0268858]
- Updated dependencies [88982d9]
  - @powersync/service-sync-rules@0.29.6
  - @powersync/service-errors@0.3.5

## 0.7.8

### Patch Changes

- da7ecc6: Upgrade mongodb driver to improve stability.
- Updated dependencies [a98cecb]
- Updated dependencies [704553e]
  - @powersync/service-sync-rules@0.29.5

## 0.7.7

### Patch Changes

- Updated dependencies [221289d]
  - @powersync/service-sync-rules@0.29.4

## 0.7.6

### Patch Changes

- f34da91: Node 22.19.0 other minor dependency updates
- Updated dependencies [f34da91]
  - @powersync/service-sync-rules@0.29.3

## 0.7.5

### Patch Changes

- Updated dependencies [17aae6d]
  - @powersync/service-sync-rules@0.29.2

## 0.7.4

### Patch Changes

- Updated dependencies [9681b4c]
  - @powersync/service-sync-rules@0.29.1

## 0.7.3

### Patch Changes

- 060b829: Update license abbreviation to FSL-1.1-ALv2.
- Updated dependencies [b0b8ae9]
- Updated dependencies [d2be184]
- Updated dependencies [5284fb5]
- Updated dependencies [18435a4]
- Updated dependencies [5284fb5]
- Updated dependencies [6fd0242]
- Updated dependencies [060b829]
  - @powersync/service-sync-rules@0.29.0
  - @powersync/service-errors@0.3.4

## 0.7.2

### Patch Changes

- Updated dependencies [1aafdaf]
  - @powersync/service-errors@0.3.3

## 0.7.1

### Patch Changes

- Updated dependencies [a60f2c7]
  - @powersync/service-errors@0.3.2

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
