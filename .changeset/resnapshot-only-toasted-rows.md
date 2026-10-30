---
'@powersync/service-module-postgres-storage': patch
---

Only resnapshot rows with missing TOAST values when a streamed update has no stored record, as the MongoDB storage already does. A complete row is evaluated and stored anyway, so the resnapshot was redundant. On a bulk update to rows not yet snapshotted this queued one resnapshot per row and kept confirmed_flush_lsn from advancing.
