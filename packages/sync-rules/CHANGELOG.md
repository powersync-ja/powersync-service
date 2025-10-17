# @powersync/service-sync-rules

## 0.29.5

### Patch Changes

- a98cecb: Sync streams: Support table aliases in subqueries.
- 704553e: Sync streams: Fix `auth.parameter()` to use top-level parameters instead of the nested `parameters` object that the legacy `token_parameters` table uses.

## 0.29.4

### Patch Changes

- 221289d: Correctly handle custom types in primary keys.

## 0.29.3

### Patch Changes

- f34da91: Node 22.19.0 other minor dependency updates

## 0.29.2

### Patch Changes

- 17aae6d: Export SQL functions for sync streams.

## 0.29.1

### Patch Changes

- 9681b4c: Add the `custom_postgres_types` compatibility option. When enabled, domain, composite, enum, range, multirange and custom array types will get synced in a JSON representation instead of the raw postgres wire format.

## 0.29.0

### Minor Changes

- b0b8ae9: Add support for streams, a new and simpler way to define what data gets synced to clients.
- d2be184: Refactor interface between service and sync rule bindings in preparation for sync streams.
- 5284fb5: Introduce the `config` option on sync rules which can be used to opt-in to new features and backwards-incompatible fixes of historical issues with the PowerSync service.
- 18435a4: Add the `fixed_json_extract` compatibility option. When enabled, JSON-extracting operators are updated to match SQLite more closely.
- 5284fb5: Add the `timestamps_iso8601` option in the `config:` block for sync rules. When enabled, timestamps are consistently formatted using ISO 8601 format.
- 6fd0242: Add the `versioned_bucket_ids` option in the `config:` block for sync rules. When enabled, generated bucket ids include the version of sync rules. This allows clients to sync more efficiently after updating sync rules.

### Patch Changes

- 060b829: Update license abbreviation to FSL-1.1-ALv2.
- Updated dependencies [060b829]
  - @powersync/service-jsonbig@0.17.11

## 0.28.0

### Minor Changes

- d4db4e2: MySQL:
  - Added schema change handling
  - Except for some edge cases, the following schema changes are now handled automatically:
    - Creation, renaming, dropping and truncation of tables.
    - Creation and dropping of unique indexes and primary keys.
    - Adding, modifying, dropping and renaming of table columns.
  - If a schema change cannot handled automatically, a warning with details will be logged.
  - Mismatches in table schema from the Zongji binlog listener are now handled more gracefully.
  - Replication of wildcard tables is now supported.
  - Improved logging for binlog event processing.

## 0.27.0

### Minor Changes

- 5b39039: Cleanup on internal sync rules implementation and APIs.

## 0.26.1

### Patch Changes

- 94f657d: Add additional log metadata to sync requests.

## 0.26.0

### Minor Changes

- ac6ae0d: Added Schema generators for Kotlin, Swift and DotNet

## 0.25.0

### Minor Changes

- bfece49: Cache parameter queries and buckets to reduce incremental sync overhead

## 0.24.1

### Patch Changes

- 0298720: Include local `attachments_queue` table when exporting schema for FlutterFlow.

## 0.24.0

### Minor Changes

- 436eee6: Minor optimizations to new checkpoint calulations.
- f55e36a: Replace bucket ids from queries with a description also containing a priority.

## 0.23.4

### Patch Changes

- e26e434: Fix -> operator on missing values to return null.

## 0.23.3

### Patch Changes

- 5043a82: Add schema generator for FlutterFlow library

## 0.23.2

### Patch Changes

- f049aa9: Improved error message for "null" bucket definitions.
- f049aa9: Introduce standard error codes

## 0.23.1

### Patch Changes

- fea550f: Updated ts-codec to 1.3.0 for better decode error responses

## 0.23.0

### Minor Changes

- a66be3b: add iif function to the sync rules

## 0.22.0

### Minor Changes

- a235c9f: Sync rules function to convert uuid to base64

## 0.21.0

### Minor Changes

- 57bd18b: Added ability to emit data replication events
- 57bd18b: Introduced alpha support for MySQL as a datasource for replication.
  Bunch of cleanup
- 57bd18b: - Introduced modules to the powersync service architecture
  - Core functionality has been moved to "engine" classes. Modules can register additional functionality with these engines.
  - The sync API functionality used by the routes has been abstracted to an interface. API routes are now managed by the RouterEngine.
  - Replication is managed by the ReplicationEngine and new replication data sources can be registered to the engine by modules.
  - Refactored existing Postgres replication as a module.
  - Removed Postgres specific code from the core service packages.
- 57bd18b: Support json_each as a table-valued function.
- 57bd18b: Optionally include original types in generated schemas as a comment.

## 0.20.0

### Minor Changes

- 0f90b02: Support substring and json_keys functions in sync rules

## 0.19.0

### Minor Changes

- 9e78ff1: Warn when identifiers are automatically convererted to lower case.
- 0e16938: Expand supported combinations of the IN operator

## 0.18.3

### Patch Changes

- f6b678a: Change TableV2 to Table in schema generator

## 0.18.2

### Patch Changes

- 2ae8711: Add TypeScript/TableV2 schema generator

## 0.18.1

### Patch Changes

- 876f4a0: Warn when id column is not present in "select \* from ..."

## 0.18.0

### Minor Changes

- 0a250e3: Expose basic documentation for functions
- 299becf: Support expressions on request parameters in parameter queries.
- 0a250e3: Support `request.parameters()`, `request.jwt()` and `request.user_id()`.
  Warn on potentially dangerous queries using request parameters.

### Patch Changes

- 0c2e2f5: Fix schema validation for parameter queries.

## 0.17.10

### Patch Changes

- 285f368: Initial public release
- Updated dependencies [285f368]
  - @powersync/service-jsonbig@0.17.10
