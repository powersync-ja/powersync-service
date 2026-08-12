---
'@powersync/service-module-mongodb-storage': minor
'@powersync/service-module-postgres-storage': minor
'@powersync/service-core': minor
---

Add source-owned `SourceTable` reconciliation.

`resolveTables()` now queries all overlapping persisted candidates and passes them to a
source-provided reconciler that returns compatible and incompatible tables, can return
modified compatible copies, and supplies values used for potential new records. Storage persists allowlisted
source metadata differences but never interprets them. MongoDB v1/v3 and PostgreSQL storage were
refactored to this candidate-first model (PostgreSQL gains a nullable `source_metadata` JSONB column
via migration).
