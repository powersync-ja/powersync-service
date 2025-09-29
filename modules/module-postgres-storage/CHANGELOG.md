# @powersync/service-module-postgres-storage

## 0.10.7

### Patch Changes

- Updated dependencies [d976830]
  - @powersync/service-core@1.15.7

## 0.10.6

### Patch Changes

- Updated dependencies [221289d]
  - @powersync/service-core@1.15.6
  - @powersync/service-sync-rules@0.29.4
  - @powersync/lib-services-framework@0.7.7
  - @powersync/service-jpgwire@0.21.3
  - @powersync/lib-service-postgres@0.4.11

## 0.10.5

### Patch Changes

- Updated dependencies [f34da91]
  - @powersync/service-sync-rules@0.29.3
  - @powersync/lib-services-framework@0.7.6
  - @powersync/service-jpgwire@0.21.2
  - @powersync/service-core@1.15.5
  - @powersync/lib-service-postgres@0.4.10

## 0.10.4

### Patch Changes

- Updated dependencies [17aae6d]
- Updated dependencies [a2b8bb0]
- Updated dependencies [f400b0f]
  - @powersync/service-sync-rules@0.29.2
  - @powersync/service-core@1.15.4
  - @powersync/lib-services-framework@0.7.5
  - @powersync/service-jpgwire@0.21.1
  - @powersync/lib-service-postgres@0.4.9

## 0.10.3

### Patch Changes

- Updated dependencies [9681b4c]
- Updated dependencies [9681b4c]
- Updated dependencies [f1b4cef]
  - @powersync/service-sync-rules@0.29.1
  - @powersync/service-jpgwire@0.21.0
  - @powersync/service-core@1.15.3
  - @powersync/lib-services-framework@0.7.4
  - @powersync/lib-service-postgres@0.4.8

## 0.10.2

### Patch Changes

- 725daa1: Fix rare issue of incorrect checksums on fallback after checksum query timed out.
- Updated dependencies [bec7496]
  - @powersync/service-core@1.15.2

## 0.10.1

### Patch Changes

- Updated dependencies [6352283]
- Updated dependencies [6352283]
  - @powersync/service-core@1.15.1

## 0.10.0

### Minor Changes

- 6d4a4d1: Create a persisted checksum cache when compacting buckets.
- c27e1c8: Upgrade Node, Sentry, Fastify and OpenTelemetry dependencies.

### Patch Changes

- 060b829: Update license abbreviation to FSL-1.1-ALv2.
- d49bebe: - Hooked up the MySQL binlog heartbeat events with the bucket batch keepalive mechanism.
  Heartbeat events will now update the latest keepalive timestamp in the sync rules.
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

## 0.9.0

### Minor Changes

- b1add5a: [MongoDB Storage] Compact action now also compacts parameter lookup storage.
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
  - @powersync/service-core-tests@0.11.0
  - @powersync/service-core@1.14.0
  - @powersync/service-jpgwire@0.20.1
  - @powersync/service-sync-rules@0.28.0
  - @powersync/lib-services-framework@0.7.2
  - @powersync/lib-service-postgres@0.4.6

## 0.8.4

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
  - @powersync/service-core-tests@0.10.4
  - @powersync/lib-service-postgres@0.4.5

## 0.8.3

### Patch Changes

- Updated dependencies [3e7d629]
- Updated dependencies [e8cb8db]
  - @powersync/service-core@1.13.3
  - @powersync/service-core-tests@0.10.3

## 0.8.2

### Patch Changes

- Updated dependencies [c002948]
  - @powersync/service-core@1.13.2
  - @powersync/service-core-tests@0.10.2

## 0.8.1

### Patch Changes

- Updated dependencies [1b326fb]
  - @powersync/service-core@1.13.1
  - @powersync/service-core-tests@0.10.1

## 0.8.0

### Minor Changes

- 0ccd470: Add powersync_replication_lag_seconds metric
- 951b010: Implement resuming of initial replication snapshots.
- d235f7b: [MongoDB Storage] Remove change streams on bucket storage database due to performance overhead.

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
  - @powersync/service-core-tests@0.10.0
  - @powersync/service-types@0.12.0
  - @powersync/service-jpgwire@0.20.0
  - @powersync/lib-services-framework@0.7.0
  - @powersync/lib-service-postgres@0.4.4

## 0.7.5

### Patch Changes

- 5b39039: Cleanup on internal sync rules implementation and APIs.
- Updated dependencies [100ccec]
- Updated dependencies [b57f938]
- Updated dependencies [5b39039]
  - @powersync/service-core@1.12.1
  - @powersync/service-sync-rules@0.27.0
  - @powersync/service-core-tests@0.9.5

## 0.7.4

### Patch Changes

- 94f657d: Add additional log metadata to sync requests.
- 05b9593: [Postgres Storage] Fix op_id_sequence initialization edge case
- Updated dependencies [ca0a566]
- Updated dependencies [9dc4e01]
- Updated dependencies [94f657d]
- Updated dependencies [05c24d2]
- Updated dependencies [d154682]
- Updated dependencies [c672380]
- Updated dependencies [ca0a566]
- Updated dependencies [ca0a566]
- Updated dependencies [05b9593]
- Updated dependencies [d869876]
  - @powersync/service-core@1.12.0
  - @powersync/lib-services-framework@0.6.0
  - @powersync/service-sync-rules@0.26.1
  - @powersync/lib-service-postgres@0.4.3
  - @powersync/service-types@0.11.0
  - @powersync/service-core-tests@0.9.4

## 0.7.3

### Patch Changes

- 23ec406: Fix has_more and other data batch metadata
- Updated dependencies [08f6ae8]
- Updated dependencies [23ec406]
- Updated dependencies [64e51d1]
  - @powersync/service-core@1.11.3
  - @powersync/service-core-tests@0.9.3
  - @powersync/lib-services-framework@0.5.4
  - @powersync/lib-service-postgres@0.4.2

## 0.7.2

### Patch Changes

- Updated dependencies [ac6ae0d]
  - @powersync/service-sync-rules@0.26.0
  - @powersync/service-core@1.11.2
  - @powersync/service-core-tests@0.9.2

## 0.7.1

### Patch Changes

- Updated dependencies [08e6e92]
  - @powersync/service-core@1.11.1
  - @powersync/service-core-tests@0.9.1

## 0.7.0

### Minor Changes

- d1b83ce: Refactored Metrics to use a MetricsEngine which is telemetry framework agnostic.

### Patch Changes

- Updated dependencies [d1b83ce]
  - @powersync/service-core-tests@0.9.0
  - @powersync/service-core@1.11.0
  - @powersync/service-types@0.10.0
  - @powersync/lib-service-postgres@0.4.1

## 0.6.2

### Patch Changes

- Updated dependencies [a9b79a5]
  - @powersync/service-core@1.10.2
  - @powersync/service-core-tests@0.8.2

## 0.6.1

### Patch Changes

- @powersync/service-core@1.10.1
- @powersync/service-core-tests@0.8.1

## 0.6.0

### Minor Changes

- bfece49: Cache parameter queries and buckets to reduce incremental sync overhead

### Patch Changes

- 833e8f2: [Postgres Storage] Fix issue when creating custom write checkpoints
- Updated dependencies [833e8f2]
- Updated dependencies [833e8f2]
- Updated dependencies [bfece49]
- Updated dependencies [2cb5252]
  - @powersync/service-core@1.10.0
  - @powersync/service-core-tests@0.8.0
  - @powersync/service-sync-rules@0.25.0

## 0.5.2

### Patch Changes

- Updated dependencies [f049f68]
- Updated dependencies [8601d6c]
  - @powersync/service-core@1.9.0
  - @powersync/service-core-tests@0.7.2

## 0.5.1

### Patch Changes

- Updated dependencies [7348ea0]
  - @powersync/service-core@1.8.1
  - @powersync/service-core-tests@0.7.1

## 0.5.0

### Minor Changes

- 698467c: Use bigint everywhere internally for OpId.
- ba7baeb: Make some service limits configurable.

### Patch Changes

- Updated dependencies [0298720]
- Updated dependencies [698467c]
- Updated dependencies [ba7baeb]
  - @powersync/service-sync-rules@0.24.1
  - @powersync/service-core-tests@0.7.0
  - @powersync/service-core@1.8.0
  - @powersync/lib-service-postgres@0.4.0
  - @powersync/service-types@0.9.0

## 0.4.2

### Patch Changes

- @powersync/service-core@1.7.2
- @powersync/service-core-tests@0.6.1

## 0.4.1

### Patch Changes

- 88ab679: Keep serving current data when restarting replication due to errors.
- 2f75fd7: Improve handling of some edge cases which could trigger truncating of synced tables.
- 346382e: Fix issue where compacting might fail with an "unexpected PUT operation" error.
- 346382e: Unified compacting options between storage providers.
- Updated dependencies [b4fe4ae]
- Updated dependencies [88ab679]
- Updated dependencies [2f75fd7]
- Updated dependencies [346382e]
- Updated dependencies [346382e]
- Updated dependencies [9b1868d]
  - @powersync/service-core@1.7.1
  - @powersync/lib-services-framework@0.5.3
  - @powersync/service-core-tests@0.6.0
  - @powersync/lib-service-postgres@0.3.3

## 0.4.0

### Minor Changes

- 436eee6: Minor optimizations to new checkpoint calulations.

### Patch Changes

- Updated dependencies [436eee6]
- Updated dependencies [15283d4]
- Updated dependencies [88d4cb3]
- Updated dependencies [f55e36a]
  - @powersync/service-core-tests@0.5.0
  - @powersync/service-core@1.7.0
  - @powersync/service-sync-rules@0.24.0
  - @powersync/lib-services-framework@0.5.2
  - @powersync/lib-service-postgres@0.3.2

## 0.3.1

### Patch Changes

- Updated dependencies [ffc8d98]
  - @powersync/service-core@0.18.1
  - @powersync/service-core-tests@0.4.1

## 0.3.0

### Minor Changes

- 4b43cdb: Exit replication process when sync rules are not valid; configurable with a new `sync_rules.exit_on_error` option.
- 9a9e668: Target Node.JS version 22, ES2024

### Patch Changes

- Updated dependencies [e26e434]
- Updated dependencies [4b43cdb]
- Updated dependencies [9a9e668]
  - @powersync/service-sync-rules@0.23.4
  - @powersync/service-core-tests@0.4.0
  - @powersync/service-core@0.18.0
  - @powersync/service-types@0.8.0
  - @powersync/lib-services-framework@0.5.1
  - @powersync/lib-service-postgres@0.3.1

## 0.2.1

### Patch Changes

- Updated dependencies [223f701]
  - @powersync/lib-service-postgres@0.3.0

## 0.2.0

### Minor Changes

- 23fb49f: Allowed using the same Postgres server for the replication source and sync bucket storage. This is only supported on Postgres versions newer than 14.0.
- 23fb49f: Added the ability to skip creating empty sync checkpoints if no changes were present in a batch.

### Patch Changes

- 23fb49f: Fix bug where listening to active checkpoint notifications on an ended connection could cause a crash.
- Updated dependencies [23fb49f]
  - @powersync/service-core@0.17.0
  - @powersync/service-core-tests@0.3.3

## 0.1.2

### Patch Changes

- Updated dependencies [5043a82]
  - @powersync/service-sync-rules@0.23.3
  - @powersync/service-core@0.16.1
  - @powersync/service-core-tests@0.3.2

## 0.1.1

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
  - @powersync/service-core-tests@0.3.1

## 0.1.0

### Minor Changes

- 9d9ff08: Initial release of Postgres bucket storage.

### Patch Changes

- Updated dependencies [9d9ff08]
- Updated dependencies [9d9ff08]
- Updated dependencies [9d9ff08]
  - @powersync/service-core-tests@0.3.0
  - @powersync/service-core@0.15.0
  - @powersync/lib-service-postgres@0.1.0
  - @powersync/lib-services-framework@0.4.0
