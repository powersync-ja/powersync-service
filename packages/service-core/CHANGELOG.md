# @powersync/service-core

## 1.9.0

### Minor Changes

- f049f68: [Postgres] Only flush once per replicated chunk, increasing transaction replication throughput.

### Patch Changes

- 8601d6c: [MySQL] Fix errors being hidden by ROLLBACK failure

## 1.8.1

### Patch Changes

- 7348ea0: Use slot_name_prefix from the replication connection again.

## 1.8.0

### Minor Changes

- 698467c: Use bigint everywhere internally for OpId.
- ba7baeb: Make some service limits configurable.

### Patch Changes

- Updated dependencies [0298720]
- Updated dependencies [ba7baeb]
  - @powersync/service-sync-rules@0.24.1
  - @powersync/service-types@0.9.0

## 1.7.2

## 1.7.1

### Patch Changes

- b4fe4ae: Upgrade mongodb and bson packages, removing the need for some workarounds.
- 88ab679: Keep serving current data when restarting replication due to errors.
- 2f75fd7: Improve handling of some edge cases which could trigger truncating of synced tables.
- 346382e: Unified compacting options between storage providers.
- 9b1868d: Fix missing checkpoint complete line for empty sync iterations.
- Updated dependencies [b4fe4ae]
  - @powersync/service-rsocket-router@0.0.20
  - @powersync/lib-services-framework@0.5.3

## 1.7.0

### Minor Changes

- 436eee6: Minor optimizations to new checkpoint calulations.

### Patch Changes

- 15283d4: Stream changes in priority order.
- 88d4cb3: Fix signed integer overflow issue for int64 values from MongoDB.
- Updated dependencies [436eee6]
- Updated dependencies [f55e36a]
  - @powersync/service-sync-rules@0.24.0
  - @powersync/lib-services-framework@0.5.2
  - @powersync/service-rsocket-router@0.0.19

## 0.18.1

### Patch Changes

- ffc8d98: Fix write checkpoint race condition

## 0.18.0

### Minor Changes

- 4b43cdb: Exit replication process when sync rules are not valid; configurable with a new `sync_rules.exit_on_error` option.
- 9a9e668: Target Node.JS version 22, ES2024

### Patch Changes

- Updated dependencies [e26e434]
- Updated dependencies [4b43cdb]
  - @powersync/service-sync-rules@0.23.4
  - @powersync/service-types@0.8.0
  - @powersync/lib-services-framework@0.5.1
  - @powersync/service-rsocket-router@0.0.18

## 0.17.0

### Minor Changes

- 23fb49f: Added the ability to skip creating empty sync checkpoints if no changes were present in a batch.

## 0.16.1

### Patch Changes

- Updated dependencies [5043a82]
  - @powersync/service-sync-rules@0.23.3

## 0.16.0

### Minor Changes

- 8675236: Support IPv6 for JWKS URI.
- 8675236: Allow limiting IP ranges of outgoing connections

### Patch Changes

- f049aa9: Add "test-connection" CLI command
- f049aa9: Introduce standard error codes
- Updated dependencies [f049aa9]
- Updated dependencies [8675236]
- Updated dependencies [8675236]
- Updated dependencies [f049aa9]
  - @powersync/service-sync-rules@0.23.2
  - @powersync/service-types@0.7.1
  - @powersync/lib-services-framework@0.5.0
  - @powersync/service-rsocket-router@0.0.17

## 0.15.0

### Minor Changes

- 9d9ff08: Updated BucketStorageFactory to use AsyncDisposable
- 9d9ff08: Initial release of Postgres bucket storage.

### Patch Changes

- Updated dependencies [9d9ff08]
  - @powersync/lib-services-framework@0.4.0
  - @powersync/service-rsocket-router@0.0.16

## 0.14.0

### Minor Changes

- fea550f: Moved MongoDB sync bucket storage implementation to the MongoDB module.

### Patch Changes

- fea550f: Updated ts-codec to 1.3.0 for better decode error responses
- 48320b5: MongoDB: Fix replication of undefined values causing missing documents
- Updated dependencies [fea550f]
- Updated dependencies [fea550f]
- Updated dependencies [fea550f]
  - @powersync/lib-services-framework@0.3.0
  - @powersync/service-sync-rules@0.23.1
  - @powersync/service-types@0.7.0
  - @powersync/service-rsocket-router@0.0.15

## 0.13.0

### Minor Changes

- 0bf1309: Add ECDSA support for JWTs

### Patch Changes

- 010f6e2: Fix reported metrics for storage size > 2GB.
- Updated dependencies [0bf1309]
- Updated dependencies [a66be3b]
  - @powersync/service-types@0.6.0
  - @powersync/service-sync-rules@0.23.0

## 0.12.2

### Patch Changes

- 320e646: Fix bucket parameters grouping.
- e3a9343: Reduce noise in log output

## 0.12.1

### Patch Changes

- 889ac46: Fix "BSONObj size is invalid" error during replication.

## 0.12.0

### Minor Changes

- ebc62ff: Add EdDSA support for JWTs.

### Patch Changes

- Updated dependencies [ebc62ff]
  - @powersync/service-types@0.5.0

## 0.11.0

### Minor Changes

- 62e97f3: Support resuming initial replication for Postgres.

### Patch Changes

- 8c6ce90: Workaround for Aurora Postgres write checkpoint bug
- Updated dependencies [a235c9f]
  - @powersync/service-sync-rules@0.22.0

## 0.10.1

### Patch Changes

- 2a4f020: Fix regression for missing HTTP probes. Reported in [issue](https://github.com/powersync-ja/powersync-service/issues/144).

## 0.10.0

### Minor Changes

- 35c267f: Add "supabase_jwt_secret" config option to simplify static Supabase auth.

### Patch Changes

- 2c18ad2: Fix compact action
- Updated dependencies [35c267f]
  - @powersync/service-types@0.4.0

## 0.9.0

### Minor Changes

- 57bd18b: Added ability to emit data replication events
- 57bd18b: Introduced alpha support for MySQL as a datasource for replication.
  Bunch of cleanup
- 57bd18b: Moved Write Checkpoint APIs to SyncBucketStorage
- 57bd18b: - Introduced modules to the powersync service architecture
  - Core functionality has been moved to "engine" classes. Modules can register additional functionality with these engines.
  - The sync API functionality used by the routes has been abstracted to an interface. API routes are now managed by the RouterEngine.
  - Replication is managed by the ReplicationEngine and new replication data sources can be registered to the engine by modules.
  - Refactored existing Postgres replication as a module.
  - Removed Postgres specific code from the core service packages.

### Patch Changes

- 57bd18b: Improved sync rules storage cached parsed sync rules, accommodating different parsing options where necessary.
- 57bd18b: Moved tag variable initialization in diagnostics route to ensure it is initialized before usage
- Updated dependencies [57bd18b]
- Updated dependencies [57bd18b]
- Updated dependencies [57bd18b]
- Updated dependencies [57bd18b]
- Updated dependencies [57bd18b]
- Updated dependencies [57bd18b]
- Updated dependencies [57bd18b]
  - @powersync/lib-services-framework@0.2.0
  - @powersync/service-sync-rules@0.21.0
  - @powersync/service-rsocket-router@0.0.14
  - @powersync/service-types@0.3.0

## 0.8.8

### Patch Changes

- 21de621: Add probe endpoints which can be used for system health checks.

## 0.8.7

### Patch Changes

- 6b72e6c: Improved Postgres connection port restrictions. Connections are now supported on ports >= 1024.

## 0.8.6

### Patch Changes

- 2d3bb6a: Fix "operation exceeded time limit" error
- 17a6db0: Fix storageStats error in metrics endpoint when collections don't exist.
- Updated dependencies [0f90b02]
  - @powersync/service-sync-rules@0.20.0

## 0.8.5

### Patch Changes

- 1fd50a5: Fix checksum cache edge case with compacting
- aa4eb0a: Fix "JavaScript heap out of memory" on startup (slot health check)
- Updated dependencies [9e78ff1]
- Updated dependencies [0e16938]
  - @powersync/service-sync-rules@0.19.0

## 0.8.4

### Patch Changes

- Updated dependencies [f6b678a]
  - @powersync/service-sync-rules@0.18.3

## 0.8.3

### Patch Changes

- 306b6d8: Fix hanging streams
- Updated dependencies [306b6d8]
  - @powersync/service-rsocket-router@0.0.13

## 0.8.2

### Patch Changes

- Updated dependencies [0d4432d]
  - @powersync/service-rsocket-router@0.0.12

## 0.8.1

### Patch Changes

- 8b3a9b9: Added `client_id` log to WebSocket `sync/stream` endpoint.

## 0.8.0

### Minor Changes

- da04865: Support client_id parameter and User-Agent headers.

### Patch Changes

- fcd54a9: Log stats on sync lock when reaching concurrency limit
- Updated dependencies [3291a2c]
  - @powersync/service-rsocket-router@0.0.11

## 0.7.1

### Patch Changes

- Updated dependencies [2ae8711]
  - @powersync/service-sync-rules@0.18.2

## 0.7.0

### Minor Changes

- c9ad713: Removed unused development routes

### Patch Changes

- Updated dependencies [c9ad713]
  - @powersync/service-types@0.2.0
  - @powersync/service-jpgwire@0.17.14

## 0.6.0

### Minor Changes

- 3f994ae: Added utility functions for registering routes

## 0.5.1

### Patch Changes

- bfe0e64: Fix compact command to use the correct database

## 0.5.0

### Minor Changes

- 1c1a3bf: Implement a compact command

## 0.4.2

### Patch Changes

- bdbf95c: Log user_id and sync stats for each connection
- Updated dependencies [876f4a0]
- Updated dependencies [9bff878]
  - @powersync/service-sync-rules@0.18.1
  - @powersync/service-rsocket-router@0.0.10

## 0.4.1

### Patch Changes

- 1066f86: Fixed missing route error logs
- Updated dependencies [909f71a]
  - @powersync/service-rsocket-router@0.0.9
  - @powersync/lib-services-framework@0.1.1

## 0.4.0

### Minor Changes

- 0a250e3: Support `request.parameters()`, `request.jwt()` and `request.user_id()`.
  Warn on potentially dangerous queries using request parameters.

### Patch Changes

- 299becf: Support expressions on request parameters in parameter queries.
- 2a0d2de: Add logging and hard exit to migration script
- 0c2e2f5: Fix schema validation for parameter queries.
- Updated dependencies [0a250e3]
- Updated dependencies [299becf]
- Updated dependencies [0c2e2f5]
- Updated dependencies [0a250e3]
  - @powersync/service-sync-rules@0.18.0

## 0.3.0

### Minor Changes

- cbf2683: Removed dependency for restricted packages

### Patch Changes

- 731c8bc: Fix replication issue with REPLICA IDENTITY FULL (#27).
- Updated dependencies [cbf2683]
  - @powersync/lib-services-framework@0.1.0
  - @powersync/service-rsocket-router@0.0.8

## 0.2.2

### Patch Changes

- 8245912: Fix teardown command not terminating after some errors.
- 7587a74: Fix date parsing in replication for dates further back than 100 AD.
- Updated dependencies [7587a74]
  - @powersync/service-jpgwire@0.17.13

## 0.2.1

### Patch Changes

- 4a57787:
  - Use a LRU cache for checksum computations, improving performance and reducing MongoDB database load.
  - Return zero checksums to the client instead of omitting, to help with debugging sync issues.

## 0.2.0

### Minor Changes

- 526a41a: Added support for user parameters when making a StreamingSyncRequest.

## 0.1.3

### Patch Changes

- 5f5163f: Fix performance issues and improve logging for initial snapshot replication.
- Updated dependencies [5f5163f]
  - @powersync/service-jpgwire@0.17.12

## 0.1.2

### Patch Changes

- b5f4ebf: Fix missing authentication errors for websocket sync stream requests
- Updated dependencies [b5f4ebf]
  - @powersync/service-rsocket-router@0.0.7

## 0.1.1

### Patch Changes

- 006fb8d: Updated `lru-cache` dependency minimum version to prevent downstream consumers of package using broken version.

## 0.1.0

### Minor Changes

- 3d9feb2: Added the ability to capture anonymous usage metrics

### Patch Changes

- Updated dependencies [3d9feb2]
  - @powersync/service-types@0.1.0
  - @powersync/service-jpgwire@0.17.11

## 0.0.2

### Patch Changes

- 285f368: Initial public release
- Updated dependencies [285f368]
  - @powersync/service-rsocket-router@0.0.6
  - @powersync/service-sync-rules@0.17.10
  - @powersync/service-jpgwire@0.17.10
  - @powersync/service-jsonbig@0.17.10
  - @powersync/service-types@0.0.2
