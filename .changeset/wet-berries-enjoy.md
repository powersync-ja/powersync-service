---
'@powersync/service-module-postgres-storage': minor
'@powersync/service-module-mongodb-storage': minor
'@powersync/service-core-tests': minor
'@powersync/service-module-postgres': minor
'@powersync/service-module-mongodb': minor
'@powersync/service-core': minor
'@powersync/service-module-mysql': minor
'@powersync/service-sync-rules': minor
---

MySQL:

- Added schema change handling
- Except for some edge cases, the following schema changes are now handled automatically:
  - Creation, renaming, dropping and truncation of tables.
  - Creation and dropping of unique indexes and primary keys.
  - Adding, modifying, dropping and renaming of table columns.
- If a schema change cannot handled automatically, a warning with details will be logged.
- Mismatches in table schema from the Zongji binlog listener are now handled more gracefully.
- Replication of wildcard tables is now supported.
- Improved logging for binlog event processing.
