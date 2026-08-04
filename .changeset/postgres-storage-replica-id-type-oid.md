---
'@powersync/service-module-postgres-storage': patch
---

Fix hydration of replica id column type ids in Postgres bucket storage.

`source_tables.replica_id_columns` is persisted with a `type_oid` key, but the decoding codec declared
`typeId`, so every `SourceTable` read back from a row lost its column type ids. This affected
`getSourceTableStatus()` and the tables returned for dropping, and now also the identity comparison
used by source table reconciliation. The codec matches the persisted key - no data migration is
needed.
