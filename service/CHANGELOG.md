# @powersync/service-image

## 1.10.0

### Minor Changes

- 833e8f2: [MongoDB Storage] Stream write checkpoint changes instead of polling, reducing overhead for large numbers of concurrent connections

### Patch Changes

- 833e8f2: [Postgres Storage] Fix issue when creating custom write checkpoints
- Updated dependencies [833e8f2]
- Updated dependencies [833e8f2]
- Updated dependencies [bfece49]
- Updated dependencies [2cb5252]
  - @powersync/service-module-postgres-storage@0.6.0
  - @powersync/service-core@1.10.0
  - @powersync/service-module-mongodb-storage@0.8.0
  - @powersync/service-module-postgres@0.11.0
  - @powersync/service-sync-rules@0.25.0
  - @powersync/service-module-mongodb@0.7.3
  - @powersync/service-module-mysql@0.5.3

## 1.9.0

### Minor Changes

- f049f68: [Postgres] Only flush once per replicated chunk, increasing transaction replication throughput.

### Patch Changes

- 8601d6c: [MySQL] Fix errors being hidden by ROLLBACK failure
- Updated dependencies [f049f68]
- Updated dependencies [8601d6c]
- Updated dependencies [535e708]
  - @powersync/service-module-postgres@0.10.0
  - @powersync/service-core@1.9.0
  - @powersync/service-module-mysql@0.5.2
  - @powersync/service-module-mongodb@0.7.2
  - @powersync/service-module-mongodb-storage@0.7.2
  - @powersync/service-module-postgres-storage@0.5.2

## 1.8.1

### Patch Changes

- 7d1cd98: Skip large rows, rather than causing hard replication errors
- Updated dependencies [7348ea0]
- Updated dependencies [7d1cd98]
  - @powersync/service-core@1.8.1
  - @powersync/service-module-mongodb-storage@0.7.1
  - @powersync/service-module-mongodb@0.7.1
  - @powersync/service-module-mysql@0.5.1
  - @powersync/service-module-postgres@0.9.1
  - @powersync/service-module-postgres-storage@0.5.1

## 1.8.0

### Minor Changes

- ba7baeb: Make some service limits configurable.

### Patch Changes

- Updated dependencies [0298720]
- Updated dependencies [698467c]
- Updated dependencies [698467c]
- Updated dependencies [ba7baeb]
  - @powersync/service-sync-rules@0.24.1
  - @powersync/service-module-postgres-storage@0.5.0
  - @powersync/service-module-mongodb-storage@0.7.0
  - @powersync/service-module-postgres@0.9.0
  - @powersync/service-module-mongodb@0.7.0
  - @powersync/service-core@1.8.0
  - @powersync/service-module-mysql@0.5.0
  - @powersync/service-types@0.9.0

## 1.7.2

### Patch Changes

- Updated dependencies [0dd746a]
  - @powersync/service-module-mongodb-storage@0.6.2
  - @powersync/service-module-mongodb@0.6.2
  - @powersync/service-module-mysql@0.4.2
  - @powersync/service-module-postgres@0.8.2
  - @powersync/service-core@1.7.2
  - @powersync/service-module-postgres-storage@0.4.2

## 1.7.1

### Patch Changes

- b4fe4ae: Upgrade mongodb and bson packages, removing the need for some workarounds.
- Updated dependencies [b4fe4ae]
- Updated dependencies [88ab679]
- Updated dependencies [2f75fd7]
- Updated dependencies [346382e]
- Updated dependencies [346382e]
- Updated dependencies [9b1868d]
  - @powersync/service-module-mongodb-storage@0.6.1
  - @powersync/service-rsocket-router@0.0.20
  - @powersync/service-module-mongodb@0.6.1
  - @powersync/service-core@1.7.1
  - @powersync/lib-services-framework@0.5.3
  - @powersync/service-module-postgres-storage@0.4.1
  - @powersync/service-module-postgres@0.8.1
  - @powersync/service-module-mysql@0.4.1

## 1.7.0

### Patch Changes

- 8111f1f: Upgrade to Node 22.14.0.
- 88d4cb3: Fix signed integer overflow issue for int64 values from MongoDB.
- Updated dependencies [436eee6]
- Updated dependencies [15283d4]
- Updated dependencies [88d4cb3]
- Updated dependencies [f55e36a]
  - @powersync/service-module-postgres-storage@0.4.0
  - @powersync/service-module-mongodb-storage@0.6.0
  - @powersync/service-module-postgres@0.8.0
  - @powersync/service-module-mongodb@0.6.0
  - @powersync/service-core@1.7.0
  - @powersync/service-module-mysql@0.4.0
  - @powersync/service-sync-rules@0.24.0
  - @powersync/lib-services-framework@0.5.2
  - @powersync/service-rsocket-router@0.0.19

## 1.4.1

### Patch Changes

- Updated dependencies [ffc8d98]
  - @powersync/service-module-postgres@0.7.1
  - @powersync/service-module-mongodb@0.5.1
  - @powersync/service-core@0.18.1
  - @powersync/service-module-mysql@0.3.1
  - @powersync/service-module-mongodb-storage@0.5.1
  - @powersync/service-module-postgres-storage@0.3.1

## 1.4.0

### Minor Changes

- d053e84: Added support for MongoDB resume tokens. This should help detect Change Stream error edge cases such as changing the replication connection details after replication has begun.
- 4b43cdb: Exit replication process when sync rules are not valid; configurable with a new `sync_rules.exit_on_error` option.

### Patch Changes

- Updated dependencies [e26e434]
- Updated dependencies [a4e387c]
- Updated dependencies [d053e84]
- Updated dependencies [4b43cdb]
- Updated dependencies [9a9e668]
  - @powersync/service-sync-rules@0.23.4
  - @powersync/service-module-mongodb@0.5.0
  - @powersync/service-module-postgres-storage@0.3.0
  - @powersync/service-module-mongodb-storage@0.5.0
  - @powersync/service-module-postgres@0.7.0
  - @powersync/service-core@0.18.0
  - @powersync/service-module-mysql@0.3.0
  - @powersync/service-types@0.8.0
  - @powersync/lib-services-framework@0.5.1
  - @powersync/service-rsocket-router@0.0.18

## 1.3.12

### Patch Changes

- @powersync/service-module-postgres@0.6.1
- @powersync/service-module-postgres-storage@0.2.1
- @powersync/service-module-mongodb@0.4.2
- @powersync/service-module-mysql@0.2.2

## 1.3.11

### Patch Changes

- Updated dependencies [23fb49f]
- Updated dependencies [23fb49f]
- Updated dependencies [23fb49f]
  - @powersync/service-module-postgres-storage@0.2.0
  - @powersync/service-module-postgres@0.6.0
  - @powersync/service-module-mongodb-storage@0.4.0
  - @powersync/service-core@0.17.0
  - @powersync/service-module-mongodb@0.4.2
  - @powersync/service-module-mysql@0.2.2

## 1.3.10

### Patch Changes

- Updated dependencies [5043a82]
  - @powersync/service-sync-rules@0.23.3
  - @powersync/service-module-mongodb@0.4.1
  - @powersync/service-module-mongodb-storage@0.3.2
  - @powersync/service-module-mysql@0.2.1
  - @powersync/service-module-postgres@0.5.1
  - @powersync/service-module-postgres-storage@0.1.2
  - @powersync/service-core@0.16.1

## 1.3.9

### Patch Changes

- f049aa9: Add "test-connection" CLI command
- f049aa9: Introduce standard error codes
- Updated dependencies [f049aa9]
- Updated dependencies [8675236]
- Updated dependencies [f049aa9]
- Updated dependencies [8675236]
- Updated dependencies [f049aa9]
- Updated dependencies [8675236]
- Updated dependencies [8675236]
- Updated dependencies [f049aa9]
  - @powersync/service-core@0.16.0
  - @powersync/service-sync-rules@0.23.2
  - @powersync/service-types@0.7.1
  - @powersync/service-module-mongodb@0.4.0
  - @powersync/service-module-postgres@0.5.0
  - @powersync/service-module-mysql@0.2.0
  - @powersync/lib-services-framework@0.5.0
  - @powersync/service-jpgwire@0.19.0
  - @powersync/service-module-postgres-storage@0.1.1
  - @powersync/service-module-mongodb-storage@0.3.1
  - @powersync/service-rsocket-router@0.0.17

## 1.3.8

### Patch Changes

- Updated dependencies [9d9ff08]
- Updated dependencies [9d9ff08]
- Updated dependencies [9d9ff08]
  - @powersync/service-module-mongodb-storage@0.3.0
  - @powersync/service-core@0.15.0
  - @powersync/service-module-postgres-storage@0.1.0
  - @powersync/service-module-postgres@0.4.0
  - @powersync/lib-services-framework@0.4.0
  - @powersync/service-module-mongodb@0.3.1
  - @powersync/service-module-mysql@0.1.9
  - @powersync/service-rsocket-router@0.0.16

## 1.3.7

### Patch Changes

- Updated dependencies [9709b2d]
  - @powersync/service-module-mongodb-storage@0.2.0
  - @powersync/service-module-mongodb@0.3.0
  - @powersync/service-module-mysql@0.1.8
  - @powersync/service-module-postgres@0.3.0

## 1.3.6

### Patch Changes

- 48320b5: MongoDB: Fix replication of undefined values causing missing documents
- Updated dependencies [fea550f]
- Updated dependencies [e25263c]
- Updated dependencies [fea550f]
- Updated dependencies [318f9f9]
- Updated dependencies [fea550f]
- Updated dependencies [48320b5]
- Updated dependencies [fea550f]
  - @powersync/service-core@0.14.0
  - @powersync/lib-services-framework@0.3.0
  - @powersync/service-sync-rules@0.23.1
  - @powersync/service-module-mysql@0.1.8
  - @powersync/service-module-postgres@0.3.0
  - @powersync/service-module-mongodb@0.2.0
  - @powersync/service-types@0.7.0
  - @powersync/service-module-mongodb-storage@0.1.0
  - @powersync/service-rsocket-router@0.0.15
  - @powersync/service-jpgwire@0.18.5

## 1.3.5

### Patch Changes

- cb749b9: Fix timestamp replication issues for MySQL.
- cb749b9: Fix resuming MySQL replication after a restart.
- Updated dependencies [cb749b9]
- Updated dependencies [0bf1309]
- Updated dependencies [cb749b9]
- Updated dependencies [a66be3b]
- Updated dependencies [010f6e2]
  - @powersync/service-module-mysql@0.1.7
  - @powersync/service-core@0.13.0
  - @powersync/service-types@0.6.0
  - @powersync/service-sync-rules@0.23.0
  - @powersync/service-module-mongodb@0.1.8
  - @powersync/service-module-postgres@0.2.4
  - @powersync/service-jpgwire@0.18.4

## 1.3.4

### Patch Changes

- Updated dependencies [e3a9343]
- Updated dependencies [320e646]
- Updated dependencies [e3a9343]
  - @powersync/service-module-postgres@0.2.3
  - @powersync/service-core@0.12.2
  - @powersync/service-module-mongodb@0.1.7
  - @powersync/service-module-mysql@0.1.6

## 1.3.3

### Patch Changes

- Updated dependencies [2a0eb11]
  - @powersync/service-module-postgres@0.2.2

## 1.3.2

### Patch Changes

- Updated dependencies [2043447]
  - @powersync/service-module-mongodb@0.1.6

## 1.3.1

### Patch Changes

- 889ac46: Fix "BSONObj size is invalid" error during replication.
- Updated dependencies [889ac46]
  - @powersync/service-module-postgres@0.2.1
  - @powersync/service-module-mongodb@0.1.5
  - @powersync/service-core@0.12.1
  - @powersync/service-module-mysql@0.1.5

## 1.3.0

### Minor Changes

- ebc62ff: Add EdDSA support for JWTs.
- f1e9ef3: Improve timeouts and table snapshots for Postgres initial replication.

### Patch Changes

- Updated dependencies [ebc62ff]
- Updated dependencies [f1e9ef3]
  - @powersync/service-core@0.12.0
  - @powersync/service-types@0.5.0
  - @powersync/service-module-postgres@0.2.0
  - @powersync/service-jpgwire@0.18.3
  - @powersync/service-module-mongodb@0.1.4
  - @powersync/service-module-mysql@0.1.4

## 1.2.0

### Minor Changes

- 62e97f3: Support resuming initial replication for Postgres.

### Patch Changes

- 15b2d8e: Disable SupabaseKeyCollector when a specific secret is configured.
- 0fa01ee: Fix replication lag diagnostics for Postgres.
- Updated dependencies [62e97f3]
- Updated dependencies [15b2d8e]
- Updated dependencies [0fa01ee]
- Updated dependencies [a235c9f]
- Updated dependencies [8c6ce90]
  - @powersync/service-module-postgres@0.1.0
  - @powersync/service-core@0.11.0
  - @powersync/service-sync-rules@0.22.0
  - @powersync/service-module-mongodb@0.1.3
  - @powersync/service-module-mysql@0.1.3
  - @powersync/service-jpgwire@0.18.2

## 1.1.1

### Patch Changes

- Updated dependencies [2a4f020]
  - @powersync/service-core@0.10.1
  - @powersync/service-module-mongodb@0.1.2
  - @powersync/service-module-mysql@0.1.2
  - @powersync/service-module-postgres@0.0.4

## 1.1.0

### Minor Changes

- 35c267f: Add "supabase_jwt_secret" config option to simplify static Supabase auth.

### Patch Changes

- 2c18ad2: Fix compact action
- Updated dependencies [2c18ad2]
- Updated dependencies [35c267f]
  - @powersync/service-core@0.10.0
  - @powersync/service-types@0.4.0
  - @powersync/service-module-mongodb@0.1.1
  - @powersync/service-module-mysql@0.1.1
  - @powersync/service-module-postgres@0.0.3
  - @powersync/service-jpgwire@0.18.1

## 1.0.0

### Major Changes

- 57bd18b: - Introduced modules to the powersync service architecture
  - Core functionality has been moved to "engine" classes. Modules can register additional functionality with these engines.
  - The sync API functionality used by the routes has been abstracted to an interface. API routes are now managed by the RouterEngine.
  - Replication is managed by the ReplicationEngine and new replication data sources can be registered to the engine by modules.
  - Refactored existing Postgres replication as a module.
  - Removed Postgres specific code from the core service packages.

### Minor Changes

- 57bd18b: Add MongoDB support (Alpha)

### Patch Changes

- Updated dependencies [57bd18b]
- Updated dependencies [57bd18b]
- Updated dependencies [57bd18b]
- Updated dependencies [57bd18b]
- Updated dependencies [57bd18b]
- Updated dependencies [57bd18b]
- Updated dependencies [57bd18b]
- Updated dependencies [57bd18b]
- Updated dependencies [57bd18b]
- Updated dependencies [57bd18b]
- Updated dependencies [57bd18b]
- Updated dependencies [57bd18b]
- Updated dependencies [57bd18b]
- Updated dependencies [57bd18b]
- Updated dependencies [57bd18b]
- Updated dependencies [57bd18b]
  - @powersync/service-module-mongodb@0.1.0
  - @powersync/service-module-mysql@0.1.0
  - @powersync/service-core@0.9.0
  - @powersync/lib-services-framework@0.2.0
  - @powersync/service-sync-rules@0.21.0
  - @powersync/service-module-postgres@0.0.2
  - @powersync/service-rsocket-router@0.0.14
  - @powersync/service-types@0.3.0
  - @powersync/service-jpgwire@0.18.0

## 0.5.8

### Patch Changes

- Updated dependencies [21de621]
  - @powersync/service-core@0.8.8

## 0.5.7

### Patch Changes

- Updated dependencies [6b72e6c]
  - @powersync/service-core@0.8.7

## 0.5.6

### Patch Changes

- Updated dependencies [2d3bb6a]
- Updated dependencies [17a6db0]
- Updated dependencies [0f90b02]
  - @powersync/service-core@0.8.6
  - @powersync/service-sync-rules@0.20.0

## 0.5.5

### Patch Changes

- 1fd50a5: Fix checksum cache edge case with compacting
- Updated dependencies [9e78ff1]
- Updated dependencies [1fd50a5]
- Updated dependencies [0e16938]
- Updated dependencies [aa4eb0a]
  - @powersync/service-sync-rules@0.19.0
  - @powersync/service-core@0.8.5

## 0.5.4

### Patch Changes

- Updated dependencies [f6b678a]
  - @powersync/service-sync-rules@0.18.3
  - @powersync/service-core@0.8.4

## 0.5.3

### Patch Changes

- Updated dependencies [306b6d8]
  - @powersync/service-rsocket-router@0.0.13
  - @powersync/service-core@0.8.3

## 0.5.2

### Patch Changes

- Updated dependencies [0d4432d]
  - @powersync/service-rsocket-router@0.0.12
  - @powersync/service-core@0.8.2

## 0.5.1

### Patch Changes

- Updated dependencies [8b3a9b9]
  - @powersync/service-core@0.8.1

## 0.5.0

### Minor Changes

- da04865: Support client_id parameter and User-Agent headers.

### Patch Changes

- Updated dependencies [3291a2c]
- Updated dependencies [da04865]
- Updated dependencies [fcd54a9]
  - @powersync/service-rsocket-router@0.0.11
  - @powersync/service-core@0.8.0

## 0.4.4

### Patch Changes

- Updated dependencies [2ae8711]
  - @powersync/service-sync-rules@0.18.2
  - @powersync/service-core@0.7.1

## 0.4.3

### Patch Changes

- Updated dependencies [c9ad713]
  - @powersync/service-core@0.7.0
  - @powersync/service-types@0.2.0
  - @powersync/service-jpgwire@0.17.14

## 0.4.2

### Patch Changes

- Updated dependencies [3f994ae]
  - @powersync/service-core@0.6.0

## 0.4.1

### Patch Changes

- bfe0e64: Fix compact command to use the correct database
- Updated dependencies [bfe0e64]
  - @powersync/service-core@0.5.1

## 0.4.0

### Minor Changes

- 1c1a3bf: Implement a compact command

### Patch Changes

- 2a8c614: Fix websockets not being closed on authentication error
- Updated dependencies [1c1a3bf]
  - @powersync/service-core@0.5.0

## 0.3.2

### Patch Changes

- bdbf95c: Log user_id and sync stats for each connection
- Updated dependencies [876f4a0]
- Updated dependencies [9bff878]
- Updated dependencies [bdbf95c]
  - @powersync/service-sync-rules@0.18.1
  - @powersync/service-rsocket-router@0.0.10
  - @powersync/service-core@0.4.2

## 0.3.1

### Patch Changes

- 909f71a: Fix concurrent connection limiting for websockets
- Updated dependencies [909f71a]
- Updated dependencies [1066f86]
  - @powersync/service-rsocket-router@0.0.9
  - @powersync/lib-services-framework@0.1.1
  - @powersync/service-core@0.4.1

## 0.3.0

### Minor Changes

- 0a250e3: Support `request.parameters()`, `request.jwt()` and `request.user_id()`.
  Warn on potentially dangerous queries using request parameters.

### Patch Changes

- 299becf: Support expressions on request parameters in parameter queries.
- 2a0d2de: Add logging and hard exit to migration script
- 0c2e2f5: Fix schema validation for parameter queries.
- Updated dependencies [0a250e3]
- Updated dependencies [299becf]
- Updated dependencies [2a0d2de]
- Updated dependencies [0c2e2f5]
- Updated dependencies [0a250e3]
  - @powersync/service-sync-rules@0.18.0
  - @powersync/service-core@0.4.0

## 0.2.7

### Patch Changes

- 731c8bc: Fix replication issue with REPLICA IDENTITY FULL (#27).
- Updated dependencies [731c8bc]
- Updated dependencies [cbf2683]
- Updated dependencies [cbf2683]
  - @powersync/service-core@0.3.0
  - @powersync/lib-services-framework@0.1.0
  - @powersync/service-rsocket-router@0.0.8

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
