# @powersync/service-module-mysql

## 0.1.9

### Patch Changes

- Updated dependencies [9d9ff08]
- Updated dependencies [9d9ff08]
- Updated dependencies [9d9ff08]
  - @powersync/service-core@0.15.0
  - @powersync/lib-services-framework@0.4.0

## 0.1.8

### Patch Changes

- e25263c: Added a heartbeat mechanism to the MySQL binlog listener replication connection to detect connection timeouts.
- 318f9f9: Resolved excessive memory consumption during MySQL initial replication.
- Updated dependencies [fea550f]
- Updated dependencies [fea550f]
- Updated dependencies [48320b5]
- Updated dependencies [fea550f]
  - @powersync/service-core@0.14.0
  - @powersync/lib-services-framework@0.3.0
  - @powersync/service-sync-rules@0.23.1
  - @powersync/service-types@0.7.0

## 0.1.7

### Patch Changes

- cb749b9: Fix timestamp replication issues for MySQL.
- cb749b9: Fix resuming MySQL replication after a restart.
- Updated dependencies [0bf1309]
- Updated dependencies [a66be3b]
- Updated dependencies [010f6e2]
  - @powersync/service-core@0.13.0
  - @powersync/service-types@0.6.0
  - @powersync/service-sync-rules@0.23.0

## 0.1.6

### Patch Changes

- Updated dependencies [320e646]
- Updated dependencies [e3a9343]
  - @powersync/service-core@0.12.2

## 0.1.5

### Patch Changes

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

- 57bd18b: Generate random serverId based on syncrule id for MySQL replication client
  Consolidated type mappings between snapshot and replicated values
  Enabled MySQL tests in CI
- 57bd18b: Introduced alpha support for MySQL as a datasource for replication.
  Bunch of cleanup

### Patch Changes

- 57bd18b: Fixed MySQL version checking to better handle non-semantic version strings
- 57bd18b: Fixed mysql schema json parsing
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
