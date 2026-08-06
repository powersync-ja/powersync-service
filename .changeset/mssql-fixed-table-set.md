---
'@powersync/service-module-mssql': minor
---

[Breaking Change]

Pin the replicated schema at the start of replication for SQL Server connections.

SQL Server does not deliver schema changes in the replication stream, so they can only be found by
polling, and a poll is never atomic with a commit. That leaves a gap where checkpoints are committed
against a schema that has already changed — producing a state the source database was never in, which
clients cannot detect. PowerSync therefore no longer adopts schema changes automatically.

The recommended schema-change workflow treats a sync config deployment as the consistency boundary. A
replication stream keeps the exact table set and CDC capture-instance identities it selected when it
started; it does not adopt schema or table changes in place. Tables must therefore be listed explicitly
rather than selected with wildcards.

What this means when making schema changes:

- **Add a table:** create the table, enable CDC, add its exact qualified name to the sync config, and
  deploy a new sync config. The new sync config snapshots the table before becoming active.
- **Change captured columns:** where SQL Server allows a rolling change, apply the DDL and create a second
  capture instance with the desired captured columns. Update explicit sync queries if needed, deploy a
  new sync config, wait for its snapshots to finish and for it to become active, and only then remove the
  old capture instance.
- **Make an identity-breaking change:** for a primary-key change, identity-column rename, or another
  change that requires CDC to be disabled, disable CDC, apply the DDL, re-enable CDC, update the sync
  config if needed, and deploy a new sync config. The current replication process cannot adopt the
  replacement capture instance, so this results in downtime and stops with `PSYNC_S1601`.
- **Drop a table:** remove it from the sync config, deploy a new sync config and wait for it to become
  active, and only then drop the source table. Dropping the table first stops replication with
  `PSYNC_S1603`; already-replicated data is retained until the new sync config becomes active.
- **Rename or drop/recreate a table:** update the sync config to express the intended table explicitly,
  ensure the resulting table is CDC-enabled with the expected schema and replica identity, and deploy a
  new sync config. The current replication process stops with `PSYNC_S1603` if its bound table is removed.
- **No deployment is needed** for changes that do not affect the captured row shape or replica identity,
  such as many index, constraint, or default changes. A non-identity column change also needs no PowerSync
  action if that changed column does not need to be replicated; otherwise use the rolling capture-instance
  workflow above.

This sequencing keeps the old table or capture instance available while the new sync config snapshots and
becomes active whenever a rolling transition is possible. If a pinned capture instance or bound table is
removed first, the current replication process stops and a new sync config must be deployed.
