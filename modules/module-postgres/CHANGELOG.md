# @powersync/service-module-postgres

## 0.2.4

### Patch Changes

- Updated dependencies [0bf1309]
- Updated dependencies [a66be3b]
- Updated dependencies [010f6e2]
  - @powersync/service-core@0.13.0
  - @powersync/service-types@0.6.0
  - @powersync/service-sync-rules@0.23.0
  - @powersync/service-jpgwire@0.18.4

## 0.2.3

### Patch Changes

- e3a9343: Fix replication slot recovery
- Updated dependencies [320e646]
- Updated dependencies [e3a9343]
  - @powersync/service-core@0.12.2

## 0.2.2

### Patch Changes

- 2a0eb11: Revert Postgres snapshot strategy.

## 0.2.1

### Patch Changes

- 889ac46: Fix "BSONObj size is invalid" error during replication.
- Updated dependencies [889ac46]
  - @powersync/service-core@0.12.1

## 0.2.0

### Minor Changes

- f1e9ef3: Improve timeouts and table snapshots for Postgres initial replication.

### Patch Changes

- Updated dependencies [ebc62ff]
- Updated dependencies [f1e9ef3]
  - @powersync/service-core@0.12.0
  - @powersync/service-types@0.5.0
  - @powersync/service-jpgwire@0.18.3

## 0.1.0

### Minor Changes

- 62e97f3: Support resuming initial replication for Postgres.

### Patch Changes

- 15b2d8e: Disable SupabaseKeyCollector when a specific secret is configured.
- 0fa01ee: Fix replication lag diagnostics for Postgres.
- Updated dependencies [62e97f3]
- Updated dependencies [a235c9f]
- Updated dependencies [8c6ce90]
  - @powersync/service-core@0.11.0
  - @powersync/service-sync-rules@0.22.0
  - @powersync/service-jpgwire@0.18.2

## 0.0.4

### Patch Changes

- Updated dependencies [2a4f020]
  - @powersync/service-core@0.10.1

## 0.0.3

### Patch Changes

- Updated dependencies [2c18ad2]
- Updated dependencies [35c267f]
  - @powersync/service-core@0.10.0
  - @powersync/service-types@0.4.0
  - @powersync/service-jpgwire@0.18.1

## 0.0.2

### Patch Changes

- 57bd18b: Updates from Replication events changes
- 57bd18b: - Introduced modules to the powersync service architecture
  - Core functionality has been moved to "engine" classes. Modules can register additional functionality with these engines.
  - The sync API functionality used by the routes has been abstracted to an interface. API routes are now managed by the RouterEngine.
  - Replication is managed by the ReplicationEngine and new replication data sources can be registered to the engine by modules.
  - Refactored existing Postgres replication as a module.
  - Removed Postgres specific code from the core service packages.
- Updated dependencies [57bd18b]
- Updated dependencies [57bd18b]
- Updated dependencies [57bd18b]
- Updated dependencies [57bd18b]
- Updated dependencies [57bd18b]
- Updated dependencies [57bd18b]
- Updated dependencies [57bd18b]
- Updated dependencies [57bd18b]
- Updated dependencies [57bd18b]
- Updated dependencies [57bd18b]
  - @powersync/service-core@0.9.0
  - @powersync/lib-services-framework@0.2.0
  - @powersync/service-sync-rules@0.21.0
  - @powersync/service-types@0.3.0
  - @powersync/service-jpgwire@0.18.0
