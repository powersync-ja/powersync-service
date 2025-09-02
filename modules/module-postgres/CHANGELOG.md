# @powersync/service-module-postgres

## 0.16.2

### Patch Changes

- Updated dependencies [bec7496]
  - @powersync/service-core@1.15.2

## 0.16.1

### Patch Changes

- 6352283: Fix pre-computing of checksums after intial replication causing replication timeouts
- Updated dependencies [6352283]
- Updated dependencies [6352283]
  - @powersync/service-core@1.15.1

## 0.16.0

### Minor Changes

- 6d4a4d1: Create a persisted checksum cache when compacting buckets.

### Patch Changes

- 060b829: Update license abbreviation to FSL-1.1-ALv2.
- Updated dependencies [6d4a4d1]
- Updated dependencies [b0b8ae9]
- Updated dependencies [d2be184]
- Updated dependencies [29a368e]
- Updated dependencies [c27e1c8]
- Updated dependencies [5284fb5]
- Updated dependencies [18435a4]
- Updated dependencies [5284fb5]
- Updated dependencies [f56acce]
- Updated dependencies [6fd0242]
- Updated dependencies [6315334]
- Updated dependencies [86807d0]
- Updated dependencies [060b829]
- Updated dependencies [d49bebe]
  - @powersync/service-core@1.15.0
  - @powersync/service-sync-rules@0.29.0
  - @powersync/service-types@0.13.0
  - @powersync/lib-service-postgres@0.4.7
  - @powersync/lib-services-framework@0.7.3
  - @powersync/service-jpgwire@0.20.2
  - @powersync/service-jsonbig@0.17.11

## 0.15.0

### Minor Changes

- 2378e36: Drop support for legacy Supabase keys via app.settings.jwt_secret.
- 2378e36: Add automatic support for Supabase JWT Signing Keys.
- d56eeb9: Delay switching over to new sync rules until we have a consistent checkpoint.
- d4db4e2: MySQL:
  - Added schema change handling
  - Except for some edge cases, the following schema changes are now handled automatically:
    - Creation, renaming, dropping and truncation of tables.
    - Creation and dropping of unique indexes and primary keys.
    - Adding, modifying, dropping and renaming of table columns.
  - If a schema change cannot handled automatically, a warning with details will be logged.
  - Mismatches in table schema from the Zongji binlog listener are now handled more gracefully.
  - Replication of wildcard tables is now supported.
  - Improved logging for binlog event processing.

### Patch Changes

- Updated dependencies [b1add5a]
- Updated dependencies [2378e36]
- Updated dependencies [4a34a51]
- Updated dependencies [4ebc3bf]
- Updated dependencies [2378e36]
- Updated dependencies [a882b94]
- Updated dependencies [1aafdaf]
- Updated dependencies [d56eeb9]
- Updated dependencies [d4db4e2]
  - @powersync/service-core@1.14.0
  - @powersync/service-jpgwire@0.20.1
  - @powersync/service-sync-rules@0.28.0
  - @powersync/lib-services-framework@0.7.2
  - @powersync/lib-service-postgres@0.4.6

## 0.14.4

### Patch Changes

- 71cf892: Add 'powersync' or 'powersync-storage' as the app name for database connections.
- Updated dependencies [a60f2c7]
- Updated dependencies [71cf892]
- Updated dependencies [ba1ceef]
- Updated dependencies [60bf5f9]
- Updated dependencies [f1431b6]
  - @powersync/service-core@1.13.4
  - @powersync/service-types@0.12.1
  - @powersync/lib-services-framework@0.7.1
  - @powersync/lib-service-postgres@0.4.5

## 0.14.3

### Patch Changes

- Updated dependencies [3e7d629]
- Updated dependencies [e8cb8db]
  - @powersync/service-core@1.13.3

## 0.14.2

### Patch Changes

- Updated dependencies [c002948]
  - @powersync/service-core@1.13.2

## 0.14.1

### Patch Changes

- Updated dependencies [1b326fb]
  - @powersync/service-core@1.13.1

## 0.14.0

### Minor Changes

- 0ccd470: Add powersync_replication_lag_seconds metric
- 951b010: Implement resuming of initial replication snapshots.
- d235f7b: [MongoDB Storage] Remove change streams on bucket storage database due to performance overhead.
- 08b7aa9: Add checks for RLS affecting replication.

### Patch Changes

- 1907356: Cleanly interrupt clearing of storage when the process is stopped/restarted.
- Updated dependencies [08b7aa9]
- Updated dependencies [0ccd470]
- Updated dependencies [1907356]
- Updated dependencies [08b7aa9]
- Updated dependencies [951b010]
- Updated dependencies [d235f7b]
- Updated dependencies [f9e8673]
  - @powersync/service-core@1.13.0
  - @powersync/service-types@0.12.0
  - @powersync/service-jpgwire@0.20.0
  - @powersync/lib-services-framework@0.7.0
  - @powersync/lib-service-postgres@0.4.4

## 0.13.1

### Patch Changes

- Updated dependencies [100ccec]
- Updated dependencies [b57f938]
- Updated dependencies [5b39039]
  - @powersync/service-core@1.12.1
  - @powersync/service-sync-rules@0.27.0

## 0.13.0

### Minor Changes

- 9dc4e01: Improve authentication error messages and logs

### Patch Changes

- 94f657d: Add additional log metadata to sync requests.
- Updated dependencies [ca0a566]
- Updated dependencies [9dc4e01]
- Updated dependencies [94f657d]
- Updated dependencies [05c24d2]
- Updated dependencies [d154682]
- Updated dependencies [c672380]
- Updated dependencies [ca0a566]
- Updated dependencies [ca0a566]
- Updated dependencies [d869876]
  - @powersync/service-core@1.12.0
  - @powersync/lib-services-framework@0.6.0
  - @powersync/service-sync-rules@0.26.1
  - @powersync/lib-service-postgres@0.4.3
  - @powersync/service-types@0.11.0

## 0.12.3

### Patch Changes

- Updated dependencies [08f6ae8]
- Updated dependencies [23ec406]
- Updated dependencies [64e51d1]
  - @powersync/service-core@1.11.3
  - @powersync/lib-services-framework@0.5.4
  - @powersync/lib-service-postgres@0.4.2

## 0.12.2

### Patch Changes

- Updated dependencies [ac6ae0d]
  - @powersync/service-sync-rules@0.26.0
  - @powersync/service-core@1.11.2

## 0.12.1

### Patch Changes

- Updated dependencies [08e6e92]
  - @powersync/service-core@1.11.1

## 0.12.0

### Minor Changes

- d1b83ce: Refactored Metrics to use a MetricsEngine which is telemetry framework agnostic.

### Patch Changes

- Updated dependencies [d1b83ce]
  - @powersync/service-core@1.11.0
  - @powersync/service-types@0.10.0
  - @powersync/lib-service-postgres@0.4.1

## 0.11.2

### Patch Changes

- Updated dependencies [a9b79a5]
  - @powersync/service-core@1.10.2

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
