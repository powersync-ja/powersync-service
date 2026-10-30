# @powersync/service-module-convex

## 0.4.5

### Patch Changes

- ebba2a3: Tweak replication job abort handling.
- Updated dependencies [ebba2a3]
- Updated dependencies [7cf585d]
- Updated dependencies [b64957d]
- Updated dependencies [070ef34]
- Updated dependencies [85c5e12]
- Updated dependencies [d951049]
- Updated dependencies [a8d5c22]
  - @powersync/service-core@1.28.0
  - @powersync/service-sync-rules@0.45.0
  - @powersync/service-types@0.21.0
  - @powersync/lib-services-framework@0.10.5

## 0.4.4

### Patch Changes

- @powersync/service-core@1.27.2

## 0.4.3

### Patch Changes

- 44ce423: Add `config.source_table_options` to edition 3 sync configs, including MongoDB pre-filtering expressions. Generate editor schemas with examples and report validation errors at their YAML source locations.

  MongoDB replication pre-filtering is not supported in the Open Edition; unsupported configurations report `PSYNC_S1348`. For example:

  ```yaml
  config:
    edition: 3
    source_table_options:
      orders:
        mongodb_filter_expression:
          $and:
            - $eq: ['$$doc.active', true]
            - $in: ['$$doc.status', ['pending', 'shipped']]
  streams:
    orders:
      query: SELECT * FROM orders
  ```

  Validation and deployment reject filters when the feature is unavailable. Parsing accepts an optional map of connection tags to connection types, rejecting MongoDB pre-filtering expressions on known non-MongoDB sources. Unknown connections or missing connection metadata produce advisory warnings. The service supplies connection metadata during validation, deployment, and filesystem loading. Filesystem-loaded sync configs also run source-capability checks. With `exit_on_error` disabled, configs remain available to diagnostics. Changing source-table options requires replacement processing when deploying a new sync config.

  Source capability validation returns structured `ValidationDiagnostic` entries, allowing multiple fatal issues and
  advisory warnings. Deployment and reprocessing reject fatal diagnostics while validation continues collecting
  independent findings. Existing `ReplicationError` consumers retain the same diagnostic shape.

- Updated dependencies [44ce423]
- Updated dependencies [51a1a89]
- Updated dependencies [e703ca5]
- Updated dependencies [324093e]
- Updated dependencies [286b0f8]
- Updated dependencies [51a1a89]
- Updated dependencies [57ef2f4]
- Updated dependencies [6700c71]
  - @powersync/service-sync-rules@0.44.0
  - @powersync/service-core@1.27.1
  - @powersync/service-types@0.20.0
  - @powersync/lib-services-framework@0.10.4

## 0.4.2

### Patch Changes

- Updated dependencies [0483d4e]
- Updated dependencies [28f024b]
- Updated dependencies [5078032]
- Updated dependencies [424bbc0]
- Updated dependencies [e87703a]
- Updated dependencies [b70f467]
- Updated dependencies [18f0c95]
- Updated dependencies [c60db6e]
- Updated dependencies [0483d4e]
- Updated dependencies [a8c3f15]
  - @powersync/service-core@1.27.0
  - @powersync/service-types@0.19.0
  - @powersync/service-sync-rules@0.43.0
  - @powersync/lib-services-framework@0.10.3

## 0.4.1

### Patch Changes

- Updated dependencies [f842b4e]
- Updated dependencies [5ae6f75]
  - @powersync/service-core@1.26.1
  - @powersync/service-sync-rules@0.42.0
  - @powersync/lib-services-framework@0.10.2

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
