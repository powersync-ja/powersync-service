# @powersync/service-module-mongodb

## 0.10.4

### Patch Changes

- 71cf892: Add 'powersync' or 'powersync-storage' as the app name for database connections.
- 60bf5f9: [MongoDB Replication] Fix resumeTokens going back in time on busy change streams.
- Updated dependencies [a60f2c7]
- Updated dependencies [71cf892]
- Updated dependencies [ba1ceef]
- Updated dependencies [60bf5f9]
- Updated dependencies [f1431b6]
  - @powersync/lib-service-mongodb@0.6.2
  - @powersync/service-core@1.13.4
  - @powersync/service-types@0.12.1
  - @powersync/lib-services-framework@0.7.1

## 0.10.3

### Patch Changes

- 3e7d629: Fix MongoDB initial replication with mixed \_id types.
- Updated dependencies [3e7d629]
- Updated dependencies [e8cb8db]
  - @powersync/service-core@1.13.3

## 0.10.2

### Patch Changes

- Updated dependencies [c002948]
  - @powersync/service-core@1.13.2

## 0.10.1

### Patch Changes

- Updated dependencies [1b326fb]
  - @powersync/service-core@1.13.1

## 0.10.0

### Minor Changes

- 0ccd470: Add powersync_replication_lag_seconds metric
- 951b010: Implement resuming of initial replication snapshots.

### Patch Changes

- 1907356: Cleanly interrupt clearing of storage when the process is stopped/restarted.
- Updated dependencies [08b7aa9]
- Updated dependencies [0ccd470]
- Updated dependencies [1907356]
- Updated dependencies [951b010]
- Updated dependencies [d235f7b]
- Updated dependencies [f9e8673]
  - @powersync/service-core@1.13.0
  - @powersync/service-types@0.12.0
  - @powersync/lib-services-framework@0.7.0
  - @powersync/lib-service-mongodb@0.6.1

## 0.9.1

### Patch Changes

- b57f938: [MongoDB] Fix replication batching
- Updated dependencies [100ccec]
- Updated dependencies [b57f938]
- Updated dependencies [5b39039]
  - @powersync/service-core@1.12.1
  - @powersync/service-sync-rules@0.27.0

## 0.9.0

### Minor Changes

- d154682: [MongoDB] Add support for plain "mongodb://" URIs for replica sets (multiple hostnames).

### Patch Changes

- 94f657d: Add additional log metadata to sync requests.
- Updated dependencies [ca0a566]
- Updated dependencies [9dc4e01]
- Updated dependencies [94f657d]
- Updated dependencies [05c24d2]
- Updated dependencies [d154682]
- Updated dependencies [c672380]
- Updated dependencies [ca0a566]
- Updated dependencies [ca0a566]
- Updated dependencies [d869876]
  - @powersync/service-core@1.12.0
  - @powersync/lib-services-framework@0.6.0
  - @powersync/service-sync-rules@0.26.1
  - @powersync/lib-service-mongodb@0.6.0
  - @powersync/service-types@0.11.0

## 0.8.3

### Patch Changes

- 08f6ae8: [MongoDB] Fix resume token handling when no events are received
- Updated dependencies [08f6ae8]
- Updated dependencies [23ec406]
- Updated dependencies [64e51d1]
  - @powersync/lib-service-mongodb@0.5.2
  - @powersync/service-core@1.11.3
  - @powersync/lib-services-framework@0.5.4

## 0.8.2

### Patch Changes

- Updated dependencies [ac6ae0d]
  - @powersync/service-sync-rules@0.26.0
  - @powersync/service-core@1.11.2

## 0.8.1

### Patch Changes

- Updated dependencies [08e6e92]
  - @powersync/service-core@1.11.1

## 0.8.0

### Minor Changes

- d1b83ce: Refactored Metrics to use a MetricsEngine which is telemetry framework agnostic.

### Patch Changes

- Updated dependencies [d1b83ce]
  - @powersync/service-core@1.11.0
  - @powersync/service-types@0.10.0

## 0.7.5

### Patch Changes

- Updated dependencies [a9b79a5]
  - @powersync/service-core@1.10.2

## 0.7.4

### Patch Changes

- @powersync/service-core@1.10.1

## 0.7.3

### Patch Changes

- Updated dependencies [833e8f2]
- Updated dependencies [833e8f2]
- Updated dependencies [bfece49]
- Updated dependencies [2cb5252]
  - @powersync/service-core@1.10.0
  - @powersync/service-sync-rules@0.25.0

## 0.7.2

### Patch Changes

- 535e708: Increase connection timeouts for MongoDB "Test Connection".
- Updated dependencies [f049f68]
- Updated dependencies [8601d6c]
  - @powersync/service-core@1.9.0

## 0.7.1

### Patch Changes

- Updated dependencies [7348ea0]
- Updated dependencies [7d1cd98]
  - @powersync/service-core@1.8.1
  - @powersync/lib-service-mongodb@0.5.1

## 0.7.0

### Minor Changes

- 698467c: Use bigint everywhere internally for OpId.

### Patch Changes

- Updated dependencies [0298720]
- Updated dependencies [698467c]
- Updated dependencies [ba7baeb]
  - @powersync/service-sync-rules@0.24.1
  - @powersync/service-core@1.8.0
  - @powersync/lib-service-mongodb@0.5.0
  - @powersync/service-types@0.9.0

## 0.6.2

### Patch Changes

- 0dd746a: Improve intial replication performance for MongoDB by avoiding sessions.
  - @powersync/service-core@1.7.2

## 0.6.1

### Patch Changes

- b4fe4ae: Upgrade mongodb and bson packages, removing the need for some workarounds.
- 88ab679: Keep serving current data when restarting replication due to errors.
- 2f75fd7: Improve handling of some edge cases which could trigger truncating of synced tables.
- Updated dependencies [b4fe4ae]
- Updated dependencies [88ab679]
- Updated dependencies [2f75fd7]
- Updated dependencies [346382e]
- Updated dependencies [9b1868d]
  - @powersync/service-core@1.7.1
  - @powersync/lib-services-framework@0.5.3
  - @powersync/lib-service-mongodb@0.4.3

## 0.6.0

### Minor Changes

- 436eee6: Minor optimizations to new checkpoint calulations.

### Patch Changes

- 88d4cb3: Fix signed integer overflow issue for int64 values from MongoDB.
- Updated dependencies [436eee6]
- Updated dependencies [15283d4]
- Updated dependencies [88d4cb3]
- Updated dependencies [f55e36a]
  - @powersync/service-core@1.7.0
  - @powersync/service-sync-rules@0.24.0
  - @powersync/lib-services-framework@0.5.2
  - @powersync/lib-service-mongodb@0.4.2

## 0.5.1

### Patch Changes

- ffc8d98: Fix write checkpoint race condition
- Updated dependencies [ffc8d98]
  - @powersync/service-core@0.18.1

## 0.5.0

### Minor Changes

- a4e387c: Added progress logs to initial snapshot
- d053e84: Added support for MongoDB resume tokens. This should help detect Change Stream error edge cases such as changing the replication connection details after replication has begun.
- 4b43cdb: Exit replication process when sync rules are not valid; configurable with a new `sync_rules.exit_on_error` option.

### Patch Changes

- Updated dependencies [e26e434]
- Updated dependencies [4b43cdb]
- Updated dependencies [9a9e668]
  - @powersync/service-sync-rules@0.23.4
  - @powersync/service-core@0.18.0
  - @powersync/service-types@0.8.0
  - @powersync/lib-services-framework@0.5.1
  - @powersync/lib-service-mongodb@0.4.1

## 0.4.2

### Patch Changes

- Updated dependencies [23fb49f]
  - @powersync/service-core@0.17.0

## 0.4.1

### Patch Changes

- Updated dependencies [5043a82]
  - @powersync/service-sync-rules@0.23.3
  - @powersync/service-core@0.16.1

## 0.4.0

### Minor Changes

- 8675236: Allow limiting IP ranges of outgoing connections

### Patch Changes

- f049aa9: Improved error messages for "Test Connection".
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

## 0.3.1

### Patch Changes

- Updated dependencies [9d9ff08]
- Updated dependencies [9d9ff08]
- Updated dependencies [9d9ff08]
  - @powersync/service-core@0.15.0
  - @powersync/lib-services-framework@0.4.0
  - @powersync/lib-service-mongodb@0.3.1

## 0.3.0

### Minor Changes

- 9709b2d: Shared MongoDB dependency between modules. This should help avoid potential multiple versions of MongoDB being present in a project.

### Patch Changes

- Updated dependencies [9709b2d]
  - @powersync/lib-service-mongodb@0.3.0

## 0.2.0

### Minor Changes

- fea550f: Moved MongoDB sync bucket storage implementation to the MongoDB module.

### Patch Changes

- 48320b5: MongoDB: Fix replication of undefined values causing missing documents
- Updated dependencies [fea550f]
- Updated dependencies [fea550f]
- Updated dependencies [48320b5]
- Updated dependencies [fea550f]
  - @powersync/service-core@0.14.0
  - @powersync/lib-services-framework@0.3.0
  - @powersync/service-sync-rules@0.23.1
  - @powersync/service-types@0.7.0
  - @powersync/lib-service-mongodb@0.2.0

## 0.1.8

### Patch Changes

- Updated dependencies [0bf1309]
- Updated dependencies [a66be3b]
- Updated dependencies [010f6e2]
  - @powersync/service-core@0.13.0
  - @powersync/service-types@0.6.0
  - @powersync/service-sync-rules@0.23.0

## 0.1.7

### Patch Changes

- Updated dependencies [320e646]
- Updated dependencies [e3a9343]
  - @powersync/service-core@0.12.2

## 0.1.6

### Patch Changes

- 2043447: Use short timeout for testing mongodb connections.

## 0.1.5

### Patch Changes

- 889ac46: Fix "BSONObj size is invalid" error during replication.
- Updated dependencies [889ac46]
  - @powersync/service-core@0.12.1

## 0.1.4

### Patch Changes

- Updated dependencies [ebc62ff]
  - @powersync/service-core@0.12.0
  - @powersync/service-types@0.5.0

## 0.1.3

### Patch Changes

- Updated dependencies [62e97f3]
- Updated dependencies [a235c9f]
- Updated dependencies [8c6ce90]
  - @powersync/service-core@0.11.0
  - @powersync/service-sync-rules@0.22.0

## 0.1.2

### Patch Changes

- Updated dependencies [2a4f020]
  - @powersync/service-core@0.10.1

## 0.1.1

### Patch Changes

- Updated dependencies [2c18ad2]
- Updated dependencies [35c267f]
  - @powersync/service-core@0.10.0
  - @powersync/service-types@0.4.0

## 0.1.0

### Minor Changes

- 57bd18b: Reduce permissions required for replicating a single mongodb database
- 57bd18b: Add MongoDB support (Alpha)

### Patch Changes

- 57bd18b: Fix diagnostics schema authorization issues for MongoDB
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
  - @powersync/service-core@0.9.0
  - @powersync/lib-services-framework@0.2.0
  - @powersync/service-sync-rules@0.21.0
  - @powersync/service-types@0.3.0
