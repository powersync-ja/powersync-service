# @powersync/lib-service-mongodb

## 0.5.1

### Patch Changes

- 7d1cd98: Skip large rows, rather than causing hard replication errors

## 0.5.0

### Minor Changes

- ba7baeb: Make some service limits configurable.

## 0.4.3

### Patch Changes

- b4fe4ae: Upgrade mongodb and bson packages, removing the need for some workarounds.
- Updated dependencies [b4fe4ae]
  - @powersync/lib-services-framework@0.5.3

## 0.4.2

### Patch Changes

- @powersync/lib-services-framework@0.5.2

## 0.4.1

### Patch Changes

- @powersync/lib-services-framework@0.5.1

## 0.4.0

### Minor Changes

- 8675236: Allow limiting IP ranges of outgoing connections

### Patch Changes

- f049aa9: Introduce standard error codes
- Updated dependencies [8675236]
- Updated dependencies [f049aa9]
  - @powersync/lib-services-framework@0.5.0

## 0.3.1

### Patch Changes

- Updated dependencies [9d9ff08]
  - @powersync/lib-services-framework@0.4.0

## 0.3.0

### Minor Changes

- 9709b2d: Shared MongoDB dependency between modules. This should help avoid potential multiple versions of MongoDB being present in a project.

## 0.2.0

### Minor Changes

- fea550f: Moved MongoDB sync bucket storage implementation to the MongoDB module.

### Patch Changes

- Updated dependencies [fea550f]
- Updated dependencies [fea550f]
  - @powersync/lib-services-framework@0.3.0
