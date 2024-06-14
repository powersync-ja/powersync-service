# powersync-open-service

## 0.2.6

### Patch Changes

- Updated dependencies [8245912]
- Updated dependencies [7587a74]
  - @powersync/service-core@0.2.2
  - @powersync/service-jpgwire@0.17.13

## 0.2.5

### Patch Changes

- 4a57787:
  - Use a LRU cache for checksum computations, improving performance and reducing MongoDB database load.
  - Return zero checksums to the client instead of omitting, to help with debugging sync issues.
- Updated dependencies [4a57787]
  - @powersync/service-core@0.2.1

## 0.2.4

### Patch Changes

- Updated dependencies [526a41a]
  - @powersync/service-core@0.2.0

## 0.2.3

### Patch Changes

- Updated dependencies [5f5163f]
- Updated dependencies [5f5163f]
  - @powersync/service-core@0.1.3
  - @powersync/service-jpgwire@0.17.12

## 0.2.2

### Patch Changes

- Updated dependencies [b5f4ebf]
- Updated dependencies [b5f4ebf]
  - @powersync/service-core@0.1.2
  - @powersync/service-rsocket-router@0.0.7

## 0.2.1

### Patch Changes

- Updated dependencies [006fb8d]
  - @powersync/service-core@0.1.1

## 0.2.0

### Minor Changes

- 3d9feb2: Added the ability to capture anonymous usage metrics

### Patch Changes

- Updated dependencies [3d9feb2]
  - @powersync/service-core@0.1.0
  - @powersync/service-types@0.1.0
  - @powersync/service-jpgwire@0.17.11

## 0.1.0

### Minor Changes

- 285f368: Initial public release

### Patch Changes

- Updated dependencies [285f368]
  - @powersync/service-rsocket-router@0.0.6
  - @powersync/service-core@0.0.2
  - @powersync/service-sync-rules@0.17.10
  - @powersync/service-jpgwire@0.17.10
  - @powersync/service-jsonbig@0.17.10
  - @powersync/service-types@0.0.2
