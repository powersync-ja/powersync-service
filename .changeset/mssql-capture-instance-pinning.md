---
'@powersync/service-module-mssql': minor
---

Pin MSSQL source-table bindings to a specific CDC capture instance.

A source table now persists the object ID of the CDC change table it replicates from, and restores that
binding across restarts, so a stream always replicates the capture schema it started with. Existing
bindings without this metadata are backfilled at job startup with the same capture instance the previous
streaming logic would have selected.

SQL Server allows two capture instances per table. When a newer one appears, the current replication
process keeps polling the instance it is bound to and warns. Deploy a new sync config to use the newer
instance; the active sync config can continue serving clients through the old instance while the new sync
config snapshots and catches up. Keep the old instance available until the new sync config becomes active.

If CDC is disabled for a bound table, or its capture instance is removed while the source table and replica
identity remain unchanged, replication stops with `PSYNC_S1601`. A newly configured table that cannot be
replicated yet — it does not exist, or CDC has not been enabled for it — stops replication with
`PSYNC_S1602` rather than being skipped. Dropping, recreating, or otherwise changing the source identity of
a previously bound table stops replication with `PSYNC_S1603`.
