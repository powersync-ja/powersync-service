# @powersync/service-jpgwire

## 0.20.2

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
  - @powersync/service-jsonbig@0.17.11

## 0.20.1

### Patch Changes

- a882b94: Change application_name on postgres connections from 'PowerSync' to 'powersync/{version}'.

## 0.20.0

### Minor Changes

- 951b010: Implement resuming of initial replication snapshots.

### Patch Changes

- 08b7aa9: Add types to pgwireRows.

## 0.19.0

### Minor Changes

- 8675236: Allow custom lookup function and tls_servername. Also reduce dependencies.

## 0.18.5

### Patch Changes

- Updated dependencies [fea550f]
- Updated dependencies [fea550f]
  - @powersync/service-sync-rules@0.23.1
  - @powersync/service-types@0.7.0

## 0.18.4

### Patch Changes

- Updated dependencies [0bf1309]
- Updated dependencies [a66be3b]
  - @powersync/service-types@0.6.0
  - @powersync/service-sync-rules@0.23.0

## 0.18.3

### Patch Changes

- f1e9ef3: Improve timeouts and table snapshots for Postgres initial replication.
- Updated dependencies [ebc62ff]
  - @powersync/service-types@0.5.0

## 0.18.2

### Patch Changes

- Updated dependencies [a235c9f]
  - @powersync/service-sync-rules@0.22.0

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
