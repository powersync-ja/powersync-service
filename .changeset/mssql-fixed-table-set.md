---
'@powersync/service-module-mssql': minor
---

[Breaking Change]

Pin the replicated schema at the start of replication for SQL Server connections.

SQL Server does not deliver schema changes in the replication stream, so they can only be found by
polling, and a poll is never atomic with a commit. That leaves a gap where checkpoints are committed
against a schema that has already changed — producing a state the source database was never in, which
clients cannot detect. PowerSync therefore no longer adopts schema changes automatically.

What this means when making schema changes:

- **Adding a table to replication:** deploy a new sync configuration. Table wildcards (`%`) are no
  longer supported, so list each table explicitly, and every table must exist with CDC enabled when
  replication starts.
- **Changing a table's columns:** deploy a new sync configuration. Replication continues against the
  original captured schema and warns, since the bound capture instance keeps its own column list.
- **Dropping or renaming a replicated table:** deploy a new sync configuration. PowerSync stops
  replicating the table and warns, but retains its already-replicated data.
- **Disabling and re-enabling CDC on a replicated table:** deploy a new sync configuration as a new
  replication stream. Replication stops with `PSYNC_S1601`. The binding is pinned to the CDC change
  table's object ID, and re-enabling CDC creates a new change table with a different ID, which the
  existing stream will not adopt.
