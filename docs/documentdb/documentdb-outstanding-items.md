# Azure DocumentDB for MongoDB vCore — Outstanding Items

> **DocumentDB support is experimental.** This document tracks **unresolved design-level
> issues and follow-ups** for DocumentDB replication — the things that are _not_ fixed
> by the current code and that a future contributor needs to be aware of.
>
> - User-visible behaviour differences live in [documentdb-limitations.md](./documentdb-limitations.md).
> - The checkpoint/LSN design lives in [documentdb-lsn-sentinel-checkpoints.md](./documentdb-lsn-sentinel-checkpoints.md).
> - This file is for _open_ items: known correctness gaps, detection gaps, and doc corrections still pending.

## Summary

| #   | Item                                                                   | Kind        | Severity | Fixable?                                    |
| --- | ---------------------------------------------------------------------- | ----------- | -------- | ------------------------------------------- |
| 1   | Large initial snapshot can age out of the change-feed retention window | Operational | Medium   | Yes (future work: incremental reprocessing) |

---

## 1. Large initial snapshot vs. change-feed retention

**Severity: Medium (operational).**

DocumentDB retains only a limited amount of change-feed history (a system-managed 400 MB log).
Initial replication snapshots, then resumes streaming from a position captured before the
snapshot. For a large or busy source, the snapshot can take long enough that this position
ages out of the retention window before streaming resumes — replication then restarts from
scratch and can loop.

In practice DocumentDB initial replication is currently suited to datasets small enough to
snapshot well within the retention window.

**Planned fix:** incremental reprocessing — consume the change stream from the moment the
snapshot begins, instead of resuming from a single pre-snapshot position.
