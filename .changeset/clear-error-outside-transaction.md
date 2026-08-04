---
'@powersync/service-module-postgres-storage': patch
---

Clear the replication error flag outside the flush transaction. Clearing it inside the REPEATABLE READ replication transaction conflicts with concurrent keepalive updates on the same sync_rules row, causing serialization failures that retry the entire flush when multiple writers are active.
