# @powersync/service-sync-rules

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
