# @powersync/service-sync-rules

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
