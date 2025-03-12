# @powersync/service-core-tests

## 0.7.1

### Patch Changes

- Updated dependencies [7348ea0]
  - @powersync/service-core@1.8.1

## 0.7.0

### Minor Changes

- 698467c: Use bigint everywhere internally for OpId.
- ba7baeb: Make some service limits configurable.

### Patch Changes

- Updated dependencies [0298720]
- Updated dependencies [698467c]
- Updated dependencies [ba7baeb]
  - @powersync/service-sync-rules@0.24.1
  - @powersync/service-core@1.8.0

## 0.6.1

### Patch Changes

- @powersync/service-core@1.7.2

## 0.6.0

### Minor Changes

- 346382e: Added compacting test for interleaved bucket operations.

### Patch Changes

- 346382e: Unified compacting options between storage providers.
- Updated dependencies [b4fe4ae]
- Updated dependencies [88ab679]
- Updated dependencies [2f75fd7]
- Updated dependencies [346382e]
- Updated dependencies [9b1868d]
  - @powersync/service-core@1.7.1

## 0.5.0

### Minor Changes

- 436eee6: Minor optimizations to new checkpoint calulations.

### Patch Changes

- Updated dependencies [436eee6]
- Updated dependencies [15283d4]
- Updated dependencies [88d4cb3]
- Updated dependencies [f55e36a]
  - @powersync/service-core@1.7.0
  - @powersync/service-sync-rules@0.24.0

## 0.4.1

### Patch Changes

- Updated dependencies [ffc8d98]
  - @powersync/service-core@0.18.1

## 0.4.0

### Minor Changes

- 4b43cdb: Exit replication process when sync rules are not valid; configurable with a new `sync_rules.exit_on_error` option.

### Patch Changes

- Updated dependencies [e26e434]
- Updated dependencies [4b43cdb]
- Updated dependencies [9a9e668]
  - @powersync/service-sync-rules@0.23.4
  - @powersync/service-core@0.18.0

## 0.3.3

### Patch Changes

- Updated dependencies [23fb49f]
  - @powersync/service-core@0.17.0

## 0.3.2

### Patch Changes

- Updated dependencies [5043a82]
  - @powersync/service-sync-rules@0.23.3
  - @powersync/service-core@0.16.1

## 0.3.1

### Patch Changes

- Updated dependencies [f049aa9]
- Updated dependencies [8675236]
- Updated dependencies [f049aa9]
- Updated dependencies [8675236]
- Updated dependencies [f049aa9]
  - @powersync/service-core@0.16.0
  - @powersync/service-sync-rules@0.23.2

## 0.3.0

### Minor Changes

- 9d9ff08: Updated BucketStorageFactory to use AsyncDisposable
- 9d9ff08: Improved migrations logic. Up migrations can be executed correctly after down migrations.

### Patch Changes

- Updated dependencies [9d9ff08]
- Updated dependencies [9d9ff08]
  - @powersync/service-core@0.15.0

## 0.2.0

### Minor Changes

- fea550f: Initial release of shared tests for different sync bucket storage providers

### Patch Changes

- Updated dependencies [fea550f]
- Updated dependencies [fea550f]
- Updated dependencies [48320b5]
  - @powersync/service-core@0.14.0
  - @powersync/service-sync-rules@0.23.1
