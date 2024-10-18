# @powersync/service-core

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
