# @powersync/service-module-mssql

## 0.10.2

### Patch Changes

- @powersync/service-core@1.26.1

## 0.10.1

### Patch Changes

- 4e4063b: Optimize runtime type checks, closes https://github.com/powersync-ja/powersync-service/issues/771.
- 2407f71: Apply timeouts and abortSignal handling to all S3 operations. The broad timeouts can be configured using the new `storage.object_storage.defaults_mode` option, or the `AWS_DEFAULTS_MODE` environment variable. Upgrade S3 SDK to fix further timeout issues.
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

## 0.10.0

### Minor Changes

- 3ca835d: Add custom CA certificate support for MSSQL connections.

  The new `cacert` option allows PowerSync to validate SQL Server certificates issued by a private CA
  without enabling `trustServerCertificate`. An optional `tls_servername` can be specified when the connection
  hostname differs from the name in the server certificate.

- 798d739: Pin MSSQL source-table bindings to a specific CDC capture instance.

  A source table now persists the object ID of the CDC change table it replicates from, and restores that
  binding across restarts, so a stream always replicates the capture schema it started with. Existing
  bindings without this metadata are backfilled at job startup with the same capture instance the previous
  streaming logic would have selected.

  SQL Server allows two capture instances per table. When a newer one appears, the current replication
  process keeps polling the instance it is bound to and warns. Deploy a new sync config to use the newer
  instance; the active sync config can continue serving clients through the old instance while the new sync
  config snapshots and catches up. Keep the old instance available until the new sync config becomes active.

  If CDC is disabled for a bound table, or its capture instance is removed while the source table and replica
  identity remain unchanged, replication stops with `PSYNC_S1601`. A newly configured table that cannot be
  replicated yet — it does not exist, or CDC has not been enabled for it — stops replication with
  `PSYNC_S1602` rather than being skipped. Dropping, recreating, or otherwise changing the source identity of
  a previously bound table stops replication with `PSYNC_S1603`.

- 798d739: [Breaking Change]

  Pin the replicated schema at the start of replication for SQL Server connections.

  SQL Server does not deliver schema changes in the replication stream, so they can only be found by
  polling, and a poll is never atomic with a commit. That leaves a gap where checkpoints are committed
  against a schema that has already changed — producing a state the source database was never in, which
  clients cannot detect. PowerSync therefore no longer adopts schema changes automatically.

  The recommended schema-change workflow treats a sync config deployment as the consistency boundary. A
  replication stream keeps the exact table set and CDC capture-instance identities it selected when it
  started; it does not adopt schema or table changes in place. Tables must therefore be listed explicitly
  rather than selected with wildcards.

  What this means when making schema changes:

  - **Add a table:** create the table, enable CDC, add its exact qualified name to the sync config, and
    deploy a new sync config. The new sync config snapshots the table before becoming active.
  - **Change captured columns:** where SQL Server allows a rolling change, apply the DDL and create a second
    capture instance with the desired captured columns. Update explicit sync queries if needed, deploy a
    new sync config, wait for its snapshots to finish and for it to become active, and only then remove the
    old capture instance.
  - **Make an identity-breaking change:** for a primary-key change, identity-column rename, or another
    change that requires CDC to be disabled, disable CDC, apply the DDL, re-enable CDC, update the sync
    config if needed, and deploy a new sync config. The current replication process cannot adopt the
    replacement in place, so this results in downtime. If it observes CDC being disabled first, it stops
    with `PSYNC_S1601` because its capture instance was removed. If it next reconciles after the replica
    identity has changed, it stops with `PSYNC_S1603` instead. Disabling and re-enabling CDC without changing
    the replica identity stops with `PSYNC_S1601`.
  - **Drop a table:** remove it from the sync config, deploy a new sync config and wait for it to become
    active, and only then drop the source table. Dropping the table first stops replication with
    `PSYNC_S1603`; already-replicated data is retained until the new sync config becomes active.
  - **Rename or drop/recreate a table:** update the sync config to express the intended table explicitly,
    ensure the resulting table is CDC-enabled with the expected schema and replica identity, and deploy a
    new sync config. The current replication process stops with `PSYNC_S1603` if its bound table is removed.
  - **No deployment is needed** for changes that do not affect the captured row shape or replica identity,
    such as many index, constraint, or default changes. A non-identity column change also needs no PowerSync
    action if that changed column does not need to be replicated; otherwise use the rolling capture-instance
    workflow above.

  This sequencing keeps the old table or capture instance available while the new sync config snapshots and
  becomes active whenever a rolling transition is possible. If a pinned capture instance or bound table is
  removed first, the current replication process stops and a new sync config must be deployed.

- 9d0b129: MSSQL CDCPoller improvements and fixes:

  - Ensure correct ordering of CDC results which previously could cause inconsistencies when handling deferred updates
  - Correctly count processed transactions in each polling cycle
  - CDC polling query now streams results

### Patch Changes

- Updated dependencies [27b56cb]
- Updated dependencies [798d739]
  - @powersync/service-core@1.25.0

## 0.9.0

### Minor Changes

- 087b61e: Configurable heartbeat_interval_seconds for MongoDB, Postgres, SQL Server.
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
- Updated dependencies [be42e25]
- Updated dependencies [cb4c627]
  - @powersync/service-core@1.24.0
  - @powersync/lib-services-framework@0.10.0
  - @powersync/service-types@0.17.0
  - @powersync/service-sync-rules@0.40.0
  - @powersync/service-errors@0.5.0

## 0.8.3

### Patch Changes

- Updated dependencies [ea71bf3]
- Updated dependencies [ea31f64]
- Updated dependencies [edc6ed4]
  - @powersync/service-sync-rules@0.39.0
  - @powersync/service-core@1.23.3
  - @powersync/lib-services-framework@0.9.8

## 0.8.2

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

## 0.8.1

### Patch Changes

- Updated dependencies [7e65360]
  - @powersync/service-core@1.23.1

## 0.8.0

### Minor Changes

- a91a08f: [Experimental] Enable incremental reprocessing for MongoDB source + MongoDB storage. This includes significant changes to the v3 storage format.

### Patch Changes

- Updated dependencies [a91a08f]
- Updated dependencies [184c39f]
- Updated dependencies [c3f75df]
- Updated dependencies [4bd35ea]
  - @powersync/service-core@1.23.0
  - @powersync/service-types@0.16.0
  - @powersync/service-errors@0.4.4
  - @powersync/service-sync-rules@0.38.0
  - @powersync/lib-services-framework@0.9.6

## 0.7.0

### Minor Changes

- e2bf1ad: [Internal] rework resolveTables to handle multiple SourceTables.
- 15e2466: [MongoDB] Support snapshotting concurrently with streaming in storage v3+.

### Patch Changes

- 6e2a57e: Refactor HydratedSyncConfig to support multiple SyncConfigs.
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

## 0.6.4

### Patch Changes

- 040fffd: Improve consistency of logs and error messages
- 2b19fc3: Update first-party uuid dependencies to v14.
- Updated dependencies [f20f318]
- Updated dependencies [9add445]
- Updated dependencies [17503d1]
- Updated dependencies [ad9ea06]
- Updated dependencies [01c29c3]
- Updated dependencies [8afe719]
- Updated dependencies [b8f0195]
- Updated dependencies [cdb8993]
- Updated dependencies [7c7b525]
- Updated dependencies [824e229]
- Updated dependencies [6304a21]
- Updated dependencies [040fffd]
- Updated dependencies [9e474d3]
- Updated dependencies [75174c4]
- Updated dependencies [423822c]
- Updated dependencies [2b19fc3]
  - @powersync/service-core@1.21.0
  - @powersync/service-sync-rules@0.36.0
  - @powersync/lib-services-framework@0.9.4
  - @powersync/service-types@0.15.2
  - @powersync/service-errors@0.4.3
  - @powersync/service-jsonbig@0.17.13

## 0.6.3

### Patch Changes

- Updated dependencies [41875f7]
- Updated dependencies [afc9890]
- Updated dependencies [2b72c2a]
- Updated dependencies [4611a49]
- Updated dependencies [b6a7896]
- Updated dependencies [2b72c2a]
- Updated dependencies [756746c]
  - @powersync/service-types@0.15.1
  - @powersync/service-core@1.20.5
  - @powersync/service-errors@0.4.2
  - @powersync/lib-services-framework@0.9.3
  - @powersync/service-sync-rules@0.35.0

## 0.6.2

### Patch Changes

- df451c6: Node 24.14.0 and other dependency upgrades.
- 11b4deb: Restructure `powersync_replication_lag_seconds` metric.
- Updated dependencies [df451c6]
- Updated dependencies [dea1e00]
- Updated dependencies [ada86f2]
- Updated dependencies [11b4deb]
  - @powersync/service-core@1.20.4
  - @powersync/service-sync-rules@0.34.1
  - @powersync/lib-services-framework@0.9.2

## 0.6.1

### Patch Changes

- @powersync/service-core@1.20.3

## 0.6.0

### Minor Changes

- 8d5d7ee: Added schema change detection and handling for the SQL Server adapter

### Patch Changes

- Updated dependencies [224c35e]
- Updated dependencies [acf1486]
- Updated dependencies [391c5ef]
- Updated dependencies [7ee87d4]
- Updated dependencies [99de8be]
- Updated dependencies [8d5d7ee]
- Updated dependencies [9daf965]
- Updated dependencies [4c92c24]
- Updated dependencies [3d230c2]
- Updated dependencies [206633f]
- Updated dependencies [3a0627e]
- Updated dependencies [275fd5f]
- Updated dependencies [7ce1b8e]
  - @powersync/service-sync-rules@0.34.0
  - @powersync/service-core@1.20.2
  - @powersync/service-errors@0.4.1
  - @powersync/lib-services-framework@0.9.1

## 0.5.0

### Minor Changes

- c15efc7: [Internal] Track and propagate source on buckets and parameter indexes to storage APIs.

### Patch Changes

- Updated dependencies [8c5bb3b]
- Updated dependencies [dcddcf1]
- Updated dependencies [c15efc7]
- Updated dependencies [e7152ce]
- Updated dependencies [e150c5c]
- Updated dependencies [b410924]
  - @powersync/service-errors@0.4.0
  - @powersync/service-core@1.20.1
  - @powersync/lib-services-framework@0.9.0
  - @powersync/service-sync-rules@0.33.0

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
