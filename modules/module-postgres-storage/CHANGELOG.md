# @powersync/service-module-postgres-storage

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
