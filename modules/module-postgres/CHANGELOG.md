# @powersync/service-module-postgres

## 0.11.1

### Patch Changes

- @powersync/service-core@1.10.1

## 0.11.0

### Minor Changes

- 833e8f2: [MongoDB Storage] Stream write checkpoint changes instead of polling, reducing overhead for large numbers of concurrent connections

### Patch Changes

- Updated dependencies [833e8f2]
- Updated dependencies [833e8f2]
- Updated dependencies [bfece49]
- Updated dependencies [2cb5252]
  - @powersync/service-core@1.10.0
  - @powersync/service-sync-rules@0.25.0

## 0.10.0

### Minor Changes

- f049f68: [Postgres] Only flush once per replicated chunk, increasing transaction replication throughput.

### Patch Changes

- Updated dependencies [f049f68]
- Updated dependencies [8601d6c]
  - @powersync/service-core@1.9.0

## 0.9.1

### Patch Changes

- Updated dependencies [7348ea0]
  - @powersync/service-core@1.8.1

## 0.9.0

### Minor Changes

- 698467c: Use bigint everywhere internally for OpId.

### Patch Changes

- Updated dependencies [0298720]
- Updated dependencies [698467c]
- Updated dependencies [ba7baeb]
  - @powersync/service-sync-rules@0.24.1
  - @powersync/service-core@1.8.0
  - @powersync/lib-service-postgres@0.4.0
  - @powersync/service-types@0.9.0

## 0.8.2

### Patch Changes

- @powersync/service-core@1.7.2

## 0.8.1

### Patch Changes

- 88ab679: Keep serving current data when restarting replication due to errors.
- Updated dependencies [b4fe4ae]
- Updated dependencies [88ab679]
- Updated dependencies [2f75fd7]
- Updated dependencies [346382e]
- Updated dependencies [9b1868d]
  - @powersync/service-core@1.7.1
  - @powersync/lib-services-framework@0.5.3
  - @powersync/lib-service-postgres@0.3.3

## 0.8.0

### Minor Changes

- 436eee6: Minor optimizations to new checkpoint calulations.

### Patch Changes

- Updated dependencies [436eee6]
- Updated dependencies [15283d4]
- Updated dependencies [88d4cb3]
- Updated dependencies [f55e36a]
  - @powersync/service-core@1.7.0
  - @powersync/service-sync-rules@0.24.0
  - @powersync/lib-services-framework@0.5.2
  - @powersync/lib-service-postgres@0.3.2

## 0.7.1

### Patch Changes

- ffc8d98: Fix write checkpoint race condition
- Updated dependencies [ffc8d98]
  - @powersync/service-core@0.18.1

## 0.7.0

### Minor Changes

- 4b43cdb: Exit replication process when sync rules are not valid; configurable with a new `sync_rules.exit_on_error` option.

### Patch Changes

- Updated dependencies [e26e434]
- Updated dependencies [4b43cdb]
- Updated dependencies [9a9e668]
  - @powersync/service-sync-rules@0.23.4
  - @powersync/service-core@0.18.0
  - @powersync/service-types@0.8.0
  - @powersync/lib-services-framework@0.5.1
  - @powersync/lib-service-postgres@0.3.1

## 0.6.1

### Patch Changes

- Updated dependencies [223f701]
  - @powersync/lib-service-postgres@0.3.0

## 0.6.0

### Minor Changes

- 23fb49f: Allowed using the same Postgres server for the replication source and sync bucket storage. This is only supported on Postgres versions newer than 14.0.

### Patch Changes

- Updated dependencies [23fb49f]
  - @powersync/service-core@0.17.0

## 0.5.1

### Patch Changes

- Updated dependencies [5043a82]
  - @powersync/service-sync-rules@0.23.3
  - @powersync/service-core@0.16.1

## 0.5.0

### Minor Changes

- 8675236: Allow limiting IP ranges of outgoing connections

### Patch Changes

- f049aa9: Introduce standard error codes
- Updated dependencies [f049aa9]
- Updated dependencies [8675236]
- Updated dependencies [f049aa9]
- Updated dependencies [8675236]
- Updated dependencies [8675236]
- Updated dependencies [8675236]
- Updated dependencies [f049aa9]
  - @powersync/service-core@0.16.0
  - @powersync/service-sync-rules@0.23.2
  - @powersync/service-types@0.7.1
  - @powersync/lib-service-postgres@0.2.0
  - @powersync/lib-services-framework@0.5.0
  - @powersync/service-jpgwire@0.19.0

## 0.4.0

### Minor Changes

- 9d9ff08: Initial release of Postgres bucket storage.

### Patch Changes

- Updated dependencies [9d9ff08]
- Updated dependencies [9d9ff08]
- Updated dependencies [9d9ff08]
  - @powersync/service-core@0.15.0
  - @powersync/lib-service-postgres@0.1.0
  - @powersync/lib-services-framework@0.4.0

## 0.3.0

### Minor Changes

- fea550f: Added minor typing utilities

### Patch Changes

- Updated dependencies [fea550f]
- Updated dependencies [fea550f]
- Updated dependencies [48320b5]
- Updated dependencies [fea550f]
  - @powersync/service-core@0.14.0
  - @powersync/lib-services-framework@0.3.0
  - @powersync/service-sync-rules@0.23.1
  - @powersync/service-types@0.7.0
  - @powersync/service-jpgwire@0.18.5

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
