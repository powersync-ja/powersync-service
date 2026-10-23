---
'@powersync/service-module-postgres-storage': patch
---

Fix Postgres storage initial replication getting stuck in `PSYNC_S1101` when the publication contains tables that the sync config does not use. Those tables are now marked as snapshotted while they are resolved, instead of blocking `markSnapshotDone` from completing.
