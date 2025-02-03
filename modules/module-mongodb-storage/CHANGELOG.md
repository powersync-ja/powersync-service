# @powersync/service-module-mongodb-storage

## 0.4.0

### Minor Changes

- 23fb49f: Added the ability to skip creating empty sync checkpoints if no changes were present in a batch.

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

- f049aa9: Introduce standard error codes
- Updated dependencies [f049aa9]
- Updated dependencies [8675236]
- Updated dependencies [f049aa9]
- Updated dependencies [8675236]
- Updated dependencies [8675236]
- Updated dependencies [f049aa9]
  - @powersync/service-core@0.16.0
  - @powersync/service-sync-rules@0.23.2
  - @powersync/service-types@0.7.1
  - @powersync/lib-services-framework@0.5.0
  - @powersync/lib-service-mongodb@0.4.0

## 0.3.0

### Minor Changes

- 9d9ff08: Updated BucketStorageFactory to use AsyncDisposable

### Patch Changes

- Updated dependencies [9d9ff08]
- Updated dependencies [9d9ff08]
- Updated dependencies [9d9ff08]
  - @powersync/service-core@0.15.0
  - @powersync/lib-services-framework@0.4.0
  - @powersync/lib-service-mongodb@0.3.1

## 0.2.0

### Minor Changes

- 9709b2d: Shared MongoDB dependency between modules. This should help avoid potential multiple versions of MongoDB being present in a project.

### Patch Changes

- Updated dependencies [9709b2d]
  - @powersync/lib-service-mongodb@0.3.0

## 0.1.0

### Minor Changes

- fea550f: Moved MongoDB sync bucket storage implementation to the MongoDB module.

### Patch Changes

- Updated dependencies [fea550f]
- Updated dependencies [fea550f]
- Updated dependencies [48320b5]
- Updated dependencies [fea550f]
  - @powersync/service-core@0.14.0
  - @powersync/lib-services-framework@0.3.0
  - @powersync/service-sync-rules@0.23.1
  - @powersync/service-types@0.7.0
  - @powersync/lib-service-mongodb@0.2.0
