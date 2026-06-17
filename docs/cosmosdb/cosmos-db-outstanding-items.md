# Azure Cosmos DB for MongoDB vCore — Outstanding Items

> **Cosmos DB support is experimental.** This document tracks **unresolved design-level
> issues and follow-ups** for Cosmos DB replication — the things that are _not_ fixed
> by the current code and that a future contributor needs to be aware of.
>
> - User-visible behaviour differences live in [cosmos-db-limitations.md](./cosmos-db-limitations.md).
> - The checkpoint/LSN design lives in [cosmos-db-lsn-sentinel-checkpoints.md](./cosmos-db-lsn-sentinel-checkpoints.md).
> - This file is for _open_ items: known correctness gaps, detection gaps, and doc corrections still pending.

## Summary

| #   | Item                                                                   | Kind          | Severity | Fixable?                                    |
| --- | ---------------------------------------------------------------------- | ------------- | -------- | ------------------------------------------- |
| 1   | Changing the source database is not detected                           | Detection gap | Medium   | Yes (future work)                           |
| 2   | Large initial snapshot can age out of the change-feed retention window | Operational   | Medium   | Yes (future work: incremental reprocessing) |

---

## 1. Changing the source database is not detected

**Severity: Medium (detection gap).**

Cosmos only supports cluster-level change streams, so the stream always opens on `admin`
with `allChangesForCluster` and filters namespaces in the pipeline. Cosmos resume tokens are
cluster-scoped, so they stay valid when only the database name changes.

On standard MongoDB, repointing a connection at a different source database invalidates the
stored position and forces a resync — a safeguard. On Cosmos that safeguard never fires:
replication silently continues from the old token, now filtered to the new (typically empty)
database.

This is documented for users in [cosmos-db-limitations.md](./cosmos-db-limitations.md)
("Changing the source database is not detected"), and the `resuming with a different source
database` test is skipped on Cosmos for this reason.

**Possible future fix:** persist the source database name alongside the LSN and validate it on
resume, raising `ChangeStreamInvalidatedError` on mismatch to force a resync.

---

## 2. Large initial snapshot vs. change-feed retention

**Severity: Medium (operational).**

Cosmos retains only a limited amount of change-feed history (a system-managed 400 MB log).
Initial replication snapshots, then resumes streaming from a position captured before the
snapshot. For a large or busy source, the snapshot can take long enough that this position
ages out of the retention window before streaming resumes — replication then restarts from
scratch and can loop.

In practice Cosmos initial replication is currently suited to datasets small enough to
snapshot well within the retention window.

**Planned fix:** incremental reprocessing — consume the change stream from the moment the
snapshot begins, instead of resuming from a single pre-snapshot position.
