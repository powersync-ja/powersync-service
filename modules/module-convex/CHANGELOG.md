# @powersync/service-module-convex

## 0.4.1

### Patch Changes

- @powersync/service-core@1.26.1

## 0.4.0

### Minor Changes

- 189bd9e: Add storage_version: 4 as a stable storage version.

### Patch Changes

- 4e4063b: Optimize runtime type checks, closes https://github.com/powersync-ja/powersync-service/issues/771.
- 2407f71: Apply timeouts and abortSignal handling to all S3 operations. The broad timeouts can be configured using the new `storage.object_storage.defaults_mode` option, or the `AWS_DEFAULTS_MODE` environment variable. Upgrade S3 SDK to fix further timeout issues.
- a997c88: Restructure MongoDB V3 bucket compacting.
- Updated dependencies [e11c54b]
- Updated dependencies [4b6a23e]
- Updated dependencies [189bd9e]
- Updated dependencies [bb08068]
- Updated dependencies [0077a8d]
- Updated dependencies [332d649]
- Updated dependencies [33a3c23]
- Updated dependencies [fccdff2]
- Updated dependencies [989d9e1]
- Updated dependencies [646c0de]
- Updated dependencies [cc121b3]
- Updated dependencies [4e4063b]
- Updated dependencies [2407f71]
- Updated dependencies [10131ca]
- Updated dependencies [4037e8f]
- Updated dependencies [3972bac]
- Updated dependencies [dd7a22f]
- Updated dependencies [a997c88]
  - @powersync/service-core@1.26.0
  - @powersync/service-sync-rules@0.41.0
  - @powersync/service-types@0.18.0
  - @powersync/lib-services-framework@0.10.1

## 0.3.1

### Patch Changes

- Updated dependencies [27b56cb]
- Updated dependencies [798d739]
  - @powersync/service-core@1.25.0

## 0.3.0

### Minor Changes

- 2189250: Add `/sync/checkpoint-request` for client-supplied checkpoint request ids, previously called write checkpoint ids. The route returns the stored `checkpoint_request_id`, storage now treats managed request ids as monotonic per user/client, custom checkpoint request ids continue to use the existing `checkpoint` field for backwards compatibility, and `checkpoint_requested_at` metadata lets compact jobs remove expired request-derived checkpoint records.

  This release includes storage migrations for the checkpoint request metadata. Self-hosters should run migrations as part of the upgrade.

### Patch Changes

- be42e25: Throw a clear error (`PSYNC_R2201`) when a schema wildcard is used in a table pattern with MongoDB, MySQL, SQL Server or Convex connections, instead of silently discovering no tables.
- Updated dependencies [087b61e]
- Updated dependencies [2189250]
- Updated dependencies [922f974]
- Updated dependencies [c4860c9]
- Updated dependencies [483415d]
- Updated dependencies [8daa300]
- Updated dependencies [aab068b]
- Updated dependencies [37591e9]
- Updated dependencies [be42e25]
- Updated dependencies [cb4c627]
  - @powersync/service-core@1.24.0
  - @powersync/lib-services-framework@0.10.0
  - @powersync/service-types@0.17.0
  - @powersync/service-sync-rules@0.40.0

## 0.2.3

### Patch Changes

- Updated dependencies [ea71bf3]
- Updated dependencies [ea31f64]
- Updated dependencies [edc6ed4]
  - @powersync/service-sync-rules@0.39.0
  - @powersync/service-core@1.23.3
  - @powersync/lib-services-framework@0.9.8

## 0.2.2

### Patch Changes

- e4f683d: [MongoDB Storage] Add experimental option to allow reading data from secondaries.
- Updated dependencies [71d4a0a]
- Updated dependencies [e4f683d]
- Updated dependencies [71d4a0a]
- Updated dependencies [a6ae678]
- Updated dependencies [c2edf86]
- Updated dependencies [df9ab1e]
  - @powersync/service-core@1.23.2
  - @powersync/service-sync-rules@0.38.1
  - @powersync/service-types@0.16.1
  - @powersync/lib-services-framework@0.9.7

## 0.2.1

### Patch Changes

- Updated dependencies [7e65360]
  - @powersync/service-core@1.23.1

## 0.2.0

### Minor Changes

- a91a08f: [Experimental] Enable incremental reprocessing for MongoDB source + MongoDB storage. This includes significant changes to the v3 storage format.

### Patch Changes

- Updated dependencies [a91a08f]
- Updated dependencies [184c39f]
- Updated dependencies [c3f75df]
- Updated dependencies [4bd35ea]
  - @powersync/service-core@1.23.0
  - @powersync/service-types@0.16.0
  - @powersync/service-sync-rules@0.38.0
  - @powersync/lib-services-framework@0.9.6

## 0.1.1

### Patch Changes

- a94b6c3: Initial alpha release
- 99d33d5: Normalize socket addresses to bare hostnames before IP-range validation, so direct-IP literals with any port form are recognized as IPs.
- Updated dependencies [17fd96b]
- Updated dependencies [6e2a57e]
- Updated dependencies [ec6df9f]
- Updated dependencies [99d33d5]
- Updated dependencies [cae92ce]
- Updated dependencies [5ac5345]
- Updated dependencies [15cb880]
- Updated dependencies [f2f5086]
- Updated dependencies [5b1b215]
- Updated dependencies [e2bf1ad]
- Updated dependencies [92cc83b]
- Updated dependencies [0aab0f9]
- Updated dependencies [15e2466]
- Updated dependencies [ebeaa3b]
- Updated dependencies [b116857]
- Updated dependencies [a94b6c3]
  - @powersync/service-core@1.22.0
  - @powersync/service-sync-rules@0.37.0
  - @powersync/lib-services-framework@0.9.5
