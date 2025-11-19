# @powersync/service-image

## 1.16.3

### Patch Changes

- fff0024: [Postgres] Fix custom type parsing on initial replication
- Updated dependencies [fff0024]
  - @powersync/service-module-postgres@0.16.12
  - @powersync/service-core@1.16.3
  - @powersync/lib-services-framework@0.7.10
  - @powersync/service-module-mongodb@0.12.12
  - @powersync/service-module-mongodb-storage@0.12.12
  - @powersync/service-module-mysql@0.9.12
  - @powersync/service-module-postgres-storage@0.10.12
  - @powersync/service-module-core@0.2.12
  - @powersync/service-rsocket-router@0.2.7

## 1.16.2

### Patch Changes

- d889219: Fix memory leaks when retrying replication after errors.
- 7eb7957: [Postgres] Remove usage of pg_logical_slot_peek_binary_changes due to performance issues in some cases
- Updated dependencies [b364581]
- Updated dependencies [d889219]
- Updated dependencies [0ace0d3]
- Updated dependencies [7eb7957]
- Updated dependencies [b364581]
  - @powersync/service-module-mongodb-storage@0.12.11
  - @powersync/service-core@1.16.2
  - @powersync/service-module-postgres-storage@0.10.11
  - @powersync/service-module-postgres@0.16.11
  - @powersync/service-module-mongodb@0.12.11
  - @powersync/service-module-mysql@0.9.11
  - @powersync/service-module-core@0.2.11

## 1.16.1

### Patch Changes

- c6bdb4f: [MongoDB storage] Fix migration for indexes on connection_report_events.
- Updated dependencies [c6bdb4f]
  - @powersync/service-module-mongodb-storage@0.12.10
  - @powersync/service-core@1.16.1
  - @powersync/service-module-mongodb@0.12.10
  - @powersync/service-module-mysql@0.9.10
  - @powersync/service-module-postgres@0.16.10
  - @powersync/service-module-core@0.2.10
  - @powersync/service-module-postgres-storage@0.10.10

## 1.16.0

### Minor Changes

- 80fd68b: Add SDK usage reporting support.

### Patch Changes

- e465ba7: Node 22.21.1, with support for --max-old-space-size-percentage.
- Updated dependencies [0e9aa94]
- Updated dependencies [80fd68b]
- Updated dependencies [88982d9]
  - @powersync/service-module-postgres@0.16.9
  - @powersync/service-module-postgres-storage@0.10.9
  - @powersync/service-module-mongodb-storage@0.12.9
  - @powersync/service-module-mongodb@0.12.9
  - @powersync/service-core@1.16.0
  - @powersync/service-module-mysql@0.9.9
  - @powersync/service-module-core@0.2.9
  - @powersync/lib-services-framework@0.7.9
  - @powersync/service-rsocket-router@0.2.6

## 1.15.8

### Patch Changes

- 5328802: [MongoDB Storage] Only compact modified buckets. Add index on bucket_state to handle large numbers of buckets when pre-calculating checksums or compacting, and skip small buckets.
- Updated dependencies [da7ecc6]
- Updated dependencies [5328802]
  - @powersync/service-module-mongodb-storage@0.12.8
  - @powersync/service-rsocket-router@0.2.5
  - @powersync/service-module-mongodb@0.12.8
  - @powersync/service-core@1.15.8
  - @powersync/lib-services-framework@0.7.8
  - @powersync/service-module-mysql@0.9.8
  - @powersync/service-module-postgres@0.16.8
  - @powersync/service-module-postgres-storage@0.10.8
  - @powersync/service-module-core@0.2.8

## 1.15.7

### Patch Changes

- d976830: Fix SnapshotTooOld on parameter queries in some cases.
- Updated dependencies [d976830]
  - @powersync/service-module-mongodb-storage@0.12.7
  - @powersync/service-core@1.15.7
  - @powersync/service-module-mongodb@0.12.7
  - @powersync/service-module-mysql@0.9.7
  - @powersync/service-module-postgres@0.16.7
  - @powersync/service-module-core@0.2.7
  - @powersync/service-module-postgres-storage@0.10.7

## 1.15.6

### Patch Changes

- Updated dependencies [221289d]
  - @powersync/service-module-postgres@0.16.6
  - @powersync/service-module-mongodb@0.12.6
  - @powersync/service-core@1.15.6
  - @powersync/service-module-mysql@0.9.6
  - @powersync/service-module-core@0.2.6
  - @powersync/service-module-mongodb-storage@0.12.6
  - @powersync/service-module-postgres-storage@0.10.6
  - @powersync/lib-services-framework@0.7.7
  - @powersync/service-rsocket-router@0.2.4

## 1.15.5

### Patch Changes

- f34da91: Node 22.19.0 other minor dependency updates
- Updated dependencies [f34da91]
  - @powersync/lib-services-framework@0.7.6
  - @powersync/service-module-mongodb@0.12.5
  - @powersync/service-module-mongodb-storage@0.12.5
  - @powersync/service-module-mysql@0.9.5
  - @powersync/service-module-postgres@0.16.5
  - @powersync/service-module-postgres-storage@0.10.5
  - @powersync/service-core@1.15.5
  - @powersync/service-module-core@0.2.5
  - @powersync/service-rsocket-router@0.2.3

## 1.15.4

### Patch Changes

- a2b8bb0: Avoid frequent write checkpoint lookups when the user does not have one.
- f400b0f: Protocol: Allow `null` as stream parameter.
- Updated dependencies [a2b8bb0]
- Updated dependencies [f400b0f]
  - @powersync/service-module-mongodb-storage@0.12.4
  - @powersync/service-core@1.15.4
  - @powersync/lib-services-framework@0.7.5
  - @powersync/service-module-mongodb@0.12.4
  - @powersync/service-module-mysql@0.9.4
  - @powersync/service-module-postgres@0.16.4
  - @powersync/service-module-postgres-storage@0.10.4
  - @powersync/service-module-core@0.2.4
  - @powersync/service-rsocket-router@0.2.2

## 1.15.3

### Patch Changes

- 9681b4c: Add the `custom_postgres_types` compatibility option. When enabled, domain, composite, enum, range, multirange and custom array types will get synced in a JSON representation instead of the raw postgres wire format.
- f1b4cef: Fix checksum calculation issues with large buckets.
- Updated dependencies [9681b4c]
- Updated dependencies [f1b4cef]
  - @powersync/service-module-postgres@0.16.3
  - @powersync/service-module-mongodb-storage@0.12.3
  - @powersync/service-core@1.15.3
  - @powersync/lib-services-framework@0.7.4
  - @powersync/service-module-mongodb@0.12.3
  - @powersync/service-module-mysql@0.9.3
  - @powersync/service-module-postgres-storage@0.10.3
  - @powersync/service-module-core@0.2.3
  - @powersync/service-rsocket-router@0.2.1

## 1.15.2

### Patch Changes

- bec7496: Fix "E11000 duplicate key error collection: powersync_demo.bucket_state" in some cases on sync rules deploy
- 725daa1: Fix rare issue of incorrect checksums on fallback after checksum query timed out.
- Updated dependencies [bec7496]
- Updated dependencies [725daa1]
  - @powersync/service-module-mongodb-storage@0.12.2
  - @powersync/service-core@1.15.2
  - @powersync/service-module-postgres-storage@0.10.2
  - @powersync/service-module-mongodb@0.12.2
  - @powersync/service-module-mysql@0.9.2
  - @powersync/service-module-postgres@0.16.2
  - @powersync/service-module-core@0.2.2

## 1.15.1

### Patch Changes

- 6352283: Fix pre-computing of checksums after intial replication causing replication timeouts
- 6352283: Improve performance of the compact job
- Updated dependencies [6352283]
- Updated dependencies [6352283]
  - @powersync/service-module-mongodb-storage@0.12.1
  - @powersync/service-module-postgres@0.16.1
  - @powersync/service-core@1.15.1
  - @powersync/service-module-mongodb@0.12.1
  - @powersync/service-module-mysql@0.9.1
  - @powersync/service-module-core@0.2.1
  - @powersync/service-module-postgres-storage@0.10.1

## 1.15.0

### Minor Changes

- b0b8ae9: Add support for streams, a new and simpler way to define what data gets synced to clients.
- c27e1c8: Upgrade Node, Sentry, Fastify and OpenTelemetry dependencies.
- 5284fb5: Introduce the `config` option on sync rules which can be used to opt-in to new features and backwards-incompatible fixes of historical issues with the PowerSync service.
- 18435a4: Add the `fixed_json_extract` compatibility option. When enabled, JSON-extracting operators are updated to match SQLite more closely.
- 5284fb5: Add the `timestamps_iso8601` option in the `config:` block for sync rules. When enabled, timestamps are consistently formatted using ISO 8601 format.
- f56acce: Enable permessage-deflate for websockets.
- 6fd0242: Add the `versioned_bucket_ids` option in the `config:` block for sync rules. When enabled, generated bucket ids include the version of sync rules. This allows clients to sync more efficiently after updating sync rules.
- 86807d0: Support gzip and zstd compression in http streams.

### Patch Changes

- d2be184: Refactor interface between service and sync rule bindings in preparation for sync streams.
- c44e5bb: Add attestations to Docker image.
- 6315334: [MongoDB Storage] Increase checksum timeouts
- 060b829: Update license abbreviation to FSL-1.1-ALv2.
- Updated dependencies [6d4a4d1]
- Updated dependencies [d2be184]
- Updated dependencies [29a368e]
- Updated dependencies [c27e1c8]
- Updated dependencies [f56acce]
- Updated dependencies [6315334]
- Updated dependencies [86807d0]
- Updated dependencies [060b829]
- Updated dependencies [d49bebe]
  - @powersync/service-module-postgres-storage@0.10.0
  - @powersync/service-module-mongodb-storage@0.12.0
  - @powersync/service-module-postgres@0.16.0
  - @powersync/service-module-mongodb@0.12.0
  - @powersync/service-core@1.15.0
  - @powersync/service-module-mysql@0.9.0
  - @powersync/service-module-core@0.2.0
  - @powersync/service-rsocket-router@0.2.0
  - @powersync/lib-services-framework@0.7.3

## 1.14.0

### Minor Changes

- b1add5a: [MongoDB Storage] Compact action now also compacts parameter lookup storage.
- 2378e36: Drop support for legacy Supabase keys via app.settings.jwt_secret.
- 4ebc3bf: Report lack of commits or keepalives as issues in the diagnostics api.
- 2378e36: Add automatic support for Supabase JWT Signing Keys.

### Patch Changes

- Updated dependencies [b1add5a]
- Updated dependencies [2378e36]
- Updated dependencies [4a34a51]
- Updated dependencies [4ebc3bf]
- Updated dependencies [2378e36]
- Updated dependencies [1aafdaf]
- Updated dependencies [d56eeb9]
- Updated dependencies [d4db4e2]
  - @powersync/service-module-postgres-storage@0.9.0
  - @powersync/service-module-mongodb-storage@0.11.0
  - @powersync/service-core@1.14.0
  - @powersync/service-module-postgres@0.15.0
  - @powersync/service-module-mongodb@0.11.0
  - @powersync/service-module-mysql@0.8.0
  - @powersync/service-module-core@0.1.7
  - @powersync/lib-services-framework@0.7.2
  - @powersync/service-rsocket-router@0.1.3

## 1.13.4

### Patch Changes

- a60f2c7: [MongoDB Storage] Improve error messages for checksum query timeouts
- fc87e1e: Use node:22.17.0-slim
- 71cf892: Add 'powersync' or 'powersync-storage' as the app name for database connections.
- 60bf5f9: [MongoDB Replication] Fix resumeTokens going back in time on busy change streams.
- Updated dependencies [a60f2c7]
- Updated dependencies [71cf892]
- Updated dependencies [ba1ceef]
- Updated dependencies [60bf5f9]
- Updated dependencies [f1431b6]
  - @powersync/service-module-mongodb-storage@0.10.4
  - @powersync/service-core@1.13.4
  - @powersync/service-module-postgres-storage@0.8.4
  - @powersync/service-module-mongodb@0.10.4
  - @powersync/service-module-postgres@0.14.4
  - @powersync/service-module-mysql@0.7.4
  - @powersync/lib-services-framework@0.7.1
  - @powersync/service-module-core@0.1.6
  - @powersync/service-rsocket-router@0.1.2

## 1.13.3

### Patch Changes

- 3e7d629: Fix MongoDB initial replication with mixed \_id types.
- e8cb8db: Fix websocket auth errors not correctly propagating the details, previously resulting in generic "[PSYNC_S2106] Authentication required" messages.
- Updated dependencies [3e7d629]
- Updated dependencies [e8cb8db]
  - @powersync/service-module-mongodb@0.10.3
  - @powersync/service-core@1.13.3
  - @powersync/service-module-core@0.1.5
  - @powersync/service-module-mongodb-storage@0.10.3
  - @powersync/service-module-mysql@0.7.3
  - @powersync/service-module-postgres@0.14.3
  - @powersync/service-module-postgres-storage@0.8.3

## 1.13.2

### Patch Changes

- c002948: Fix sync rule clearing process to not block sync rule processing.
- Updated dependencies [c002948]
  - @powersync/service-core@1.13.2
  - @powersync/service-module-core@0.1.4
  - @powersync/service-module-mongodb@0.10.2
  - @powersync/service-module-mongodb-storage@0.10.2
  - @powersync/service-module-mysql@0.7.2
  - @powersync/service-module-postgres@0.14.2
  - @powersync/service-module-postgres-storage@0.8.2

## 1.13.1

### Patch Changes

- 1b326fb: [MongoDB Storage] Fix checksum calculations in buckets with more than 4 million operations
- Updated dependencies [1b326fb]
  - @powersync/service-module-mongodb-storage@0.10.1
  - @powersync/service-core@1.13.1
  - @powersync/service-module-mongodb@0.10.1
  - @powersync/service-module-mysql@0.7.1
  - @powersync/service-module-postgres@0.14.1
  - @powersync/service-module-core@0.1.3
  - @powersync/service-module-postgres-storage@0.8.1

## 1.13.0

### Patch Changes

- 08b7aa9: Add checks for RLS affecting replication.
- Updated dependencies [08b7aa9]
- Updated dependencies [0ccd470]
- Updated dependencies [e11754d]
- Updated dependencies [1907356]
- Updated dependencies [951b010]
- Updated dependencies [d235f7b]
- Updated dependencies [08b7aa9]
- Updated dependencies [f9e8673]
  - @powersync/service-core@1.13.0
  - @powersync/service-module-postgres-storage@0.8.0
  - @powersync/service-module-mongodb-storage@0.10.0
  - @powersync/service-module-postgres@0.14.0
  - @powersync/service-module-mongodb@0.10.0
  - @powersync/service-module-mysql@0.7.0
  - @powersync/lib-services-framework@0.7.0
  - @powersync/service-module-core@0.1.2
  - @powersync/service-rsocket-router@0.1.1

## 1.12.1

### Patch Changes

- b57f938: [MongoDB] Fix replication batching
- Updated dependencies [100ccec]
- Updated dependencies [b57f938]
- Updated dependencies [5b39039]
  - @powersync/service-core@1.12.1
  - @powersync/service-module-mongodb@0.9.1
  - @powersync/service-module-postgres-storage@0.7.5
  - @powersync/service-module-mongodb-storage@0.9.5
  - @powersync/service-module-core@0.1.1
  - @powersync/service-module-mysql@0.6.5
  - @powersync/service-module-postgres@0.13.1

## 1.12.0

### Minor Changes

- 9dc4e01: Improve authentication error messages and logs
- 94f657d: Add additional log metadata to sync requests.
- d154682: [MongoDB] Add support for plain "mongodb://" URIs for replica sets (multiple hostnames).
- a602fb2: Support WebSocket requests to be encoded as JSON, which will enable more SDKs to use WebSockets as a transport protocol when receiving sync lines.
- ca0a566: - Added typecasting to `!env` YAML custom tag function. YAML config environment variable substitution now supports casting string environment variables to `number` and `boolean` types.

  ```yaml
  replication:
    connections: []

  storage:
    type: mongodb

  api:
    parameters:
      max_buckets_per_connection: !env PS_MAX_BUCKETS::number

  healthcheck:
    probes:
      use_http: !env PS_MONGO_HEALTHCHECK::boolean
  ```

  - Added the ability to customize healthcheck probe exposure in the configuration. Backwards compatibility is maintained if no `healthcheck->probes` config is provided.

  ```yaml
  healthcheck:
    probes:
      # Health status can be accessed by reading files (previously always enabled)
      use_filesystem: true
      # Health status can be accessed via HTTP requests (previously enabled for API and UNIFIED service modes)
      use_http: true
  ```

### Patch Changes

- 05b9593: [Postgres Storage] Fix op_id_sequence initialization edge case
- Updated dependencies [ca0a566]
- Updated dependencies [9dc4e01]
- Updated dependencies [94f657d]
- Updated dependencies [05c24d2]
- Updated dependencies [d154682]
- Updated dependencies [c672380]
- Updated dependencies [ca0a566]
- Updated dependencies [05b9593]
- Updated dependencies [ca0a566]
- Updated dependencies [d869876]
  - @powersync/service-core@1.12.0
  - @powersync/service-module-postgres@0.13.0
  - @powersync/service-rsocket-router@0.1.0
  - @powersync/lib-services-framework@0.6.0
  - @powersync/service-module-postgres-storage@0.7.4
  - @powersync/service-module-mongodb-storage@0.9.4
  - @powersync/service-module-mongodb@0.9.0
  - @powersync/service-module-mysql@0.6.4
  - @powersync/service-module-core@0.1.0

## 1.11.3

### Patch Changes

- 08f6ae8: [MongoDB] Fix resume token handling when no events are received
- 23ec406: Fix has_more and other data batch metadata
- Updated dependencies [08f6ae8]
- Updated dependencies [23ec406]
- Updated dependencies [64e51d1]
  - @powersync/service-module-mongodb@0.8.3
  - @powersync/service-core@1.11.3
  - @powersync/service-module-postgres-storage@0.7.3
  - @powersync/service-module-mongodb-storage@0.9.3
  - @powersync/lib-services-framework@0.5.4
  - @powersync/service-module-mysql@0.6.3
  - @powersync/service-module-postgres@0.12.3
  - @powersync/service-rsocket-router@0.0.21

## 1.11.2

### Patch Changes

- @powersync/service-module-mongodb@0.8.2
- @powersync/service-module-mongodb-storage@0.9.2
- @powersync/service-module-mysql@0.6.2
- @powersync/service-module-postgres@0.12.2
- @powersync/service-module-postgres-storage@0.7.2
- @powersync/service-core@1.11.2

## 1.11.1

### Patch Changes

- 08e6e92: Fix slow clearing of bucket_parameters collection.
- Updated dependencies [08e6e92]
  - @powersync/service-module-mongodb-storage@0.9.1
  - @powersync/service-core@1.11.1
  - @powersync/service-module-mongodb@0.8.1
  - @powersync/service-module-mysql@0.6.1
  - @powersync/service-module-postgres@0.12.1
  - @powersync/service-module-postgres-storage@0.7.1

## 1.11.0

### Patch Changes

- Updated dependencies [d1b83ce]
  - @powersync/service-module-postgres-storage@0.7.0
  - @powersync/service-module-mongodb-storage@0.9.0
  - @powersync/service-module-postgres@0.12.0
  - @powersync/service-module-mongodb@0.8.0
  - @powersync/service-core@1.11.0
  - @powersync/service-module-mysql@0.6.0

## 1.10.2

### Patch Changes

- Updated dependencies [a9b79a5]
  - @powersync/service-core@1.10.2
  - @powersync/service-module-mongodb@0.7.5
  - @powersync/service-module-mongodb-storage@0.8.2
  - @powersync/service-module-mysql@0.5.5
  - @powersync/service-module-postgres@0.11.2
  - @powersync/service-module-postgres-storage@0.6.2

## 1.10.1

### Patch Changes

- Updated dependencies [a6dee95]
  - @powersync/service-module-mysql@0.5.4
  - @powersync/service-core@1.10.1
  - @powersync/service-module-mongodb@0.7.4
  - @powersync/service-module-mongodb-storage@0.8.1
  - @powersync/service-module-postgres@0.11.1
  - @powersync/service-module-postgres-storage@0.6.1

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
