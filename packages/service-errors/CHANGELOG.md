# @powersync/service-errors

## 0.3.1

### Patch Changes

- 951b010: Implement resuming of initial replication snapshots.
- f9e8673: [MongoDB Storage] Handle connection errors on startup

## 0.3.0

### Minor Changes

- 9dc4e01: Improve authentication error messages and logs

### Patch Changes

- d869876: Allow RSocket request payload to be encoded as JSON

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
