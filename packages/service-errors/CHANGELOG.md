# @powersync/service-errors

## 0.2.2

### Patch Changes

- 08f6ae8: [MongoDB] Fix resume token handling when no events are received

## 0.2.1

### Patch Changes

- 436eee6: Minor optimizations to new checkpoint calulations.

## 0.2.0

### Minor Changes

- d053e84: Added support for MongoDB resume tokens. This should help detect Change Stream error edge cases such as changing the replication connection details after replication has begun.

## 0.1.1

### Patch Changes

- f049aa9: Introduce standard error codes
