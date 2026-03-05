# @powersync/service-module-mssql

## 0.4.0

### Minor Changes

- 8bd83e8: Introduce storage versions.

### Patch Changes

- Updated dependencies [0998251]
- Updated dependencies [65f3c89]
- Updated dependencies [1c45667]
- Updated dependencies [8785a3f]
- Updated dependencies [8a4c34e]
- Updated dependencies [b440093]
- Updated dependencies [d7ff4ad]
- Updated dependencies [c683322]
- Updated dependencies [8bd83e8]
- Updated dependencies [83989b2]
- Updated dependencies [79a9729]
- Updated dependencies [5edd95f]
  - @powersync/service-core@1.20.0
  - @powersync/service-types@0.15.0
  - @powersync/service-sync-rules@0.32.0
  - @powersync/service-errors@0.3.7
  - @powersync/lib-services-framework@0.8.3

## 0.3.1

### Patch Changes

- Updated dependencies [a04252d]
  - @powersync/service-sync-rules@0.31.1
  - @powersync/lib-services-framework@0.8.2
  - @powersync/service-core@1.19.2

## 0.3.0

### Minor Changes

- e11289d: Support connections to SQL Server 2019

### Patch Changes

- Updated dependencies [0e99ce0]
- Updated dependencies [479997b]
- Updated dependencies [d1c2228]
- Updated dependencies [1a1a4cc]
  - @powersync/service-sync-rules@0.31.0
  - @powersync/service-core@1.19.1
  - @powersync/lib-services-framework@0.8.1

## 0.2.0

### Minor Changes

- e578245: [Internal] Refactor sync rule representation to split out the parsed definitions from the hydrated state.

### Patch Changes

- Updated dependencies [05b9661]
- Updated dependencies [eaa04cc]
- Updated dependencies [781d0e3]
- Updated dependencies [e578245]
- Updated dependencies [3040079]
- Updated dependencies [3b2c512]
- Updated dependencies [a02cc58]
  - @powersync/service-core@1.19.0
  - @powersync/service-sync-rules@0.30.0
  - @powersync/lib-services-framework@0.8.0
  - @powersync/service-types@0.14.0

## 0.1.2

### Patch Changes

- bdfd287: Add the `timestamp_max_precision` option for sync rules. It can be set to `seconds`, `milliseconds` or `microseconds` to restrict the precision of synced datetime values.
- Updated dependencies [8fdbf8d]
- Updated dependencies [bdfd287]
  - @powersync/service-core@1.18.2
  - @powersync/service-sync-rules@0.29.10
  - @powersync/lib-services-framework@0.7.14

## 0.1.1

### Patch Changes

- 21b3a41: Fixed sync rule validation query for mssql
- Updated dependencies [21b3a41]
  - @powersync/service-sync-rules@0.29.9
  - @powersync/lib-services-framework@0.7.13
  - @powersync/service-core@1.18.1

## 0.1.0

### Minor Changes

- b77bb2c: - First iteration of MSSQL replication using Change Data Capture (CDC).
  - Supports resumable snapshot replication
  - Uses CDC polling for replication

### Patch Changes

- Updated dependencies [dc696b1]
- Updated dependencies [b77bb2c]
  - @powersync/service-core@1.18.0
  - @powersync/service-types@0.13.3
  - @powersync/service-errors@0.3.6
  - @powersync/lib-services-framework@0.7.12
