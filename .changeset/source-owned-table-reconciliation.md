---
'@powersync/service-module-mongodb-storage': minor
'@powersync/service-module-postgres-storage': minor
'@powersync/service-core': minor
---

Add source-owned `SourceTable` reconciliation.

`resolveTables()` now queries all overlapping persisted candidates and passes them to a
source-provided reconciler that classifies compatibility and selects an opaque, source-specific
`sourceMetadata` value to persist. Storage persists and hydrates this metadata but never interprets
it. MongoDB v1/v3 and PostgreSQL storage were refactored to this candidate-first model (PostgreSQL
gains a nullable `source_metadata` JSONB column via migration).
